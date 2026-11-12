// Live data layer: Firebase Auth (Google) + Firestore with offline cache.
// The UI lives in index.html and talks to this module through a small API.
import { initializeApp } from 'https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js';
import {
  getAuth, GoogleAuthProvider, signInWithPopup, signInWithRedirect, getRedirectResult,
  onAuthStateChanged, signOut as fbSignOut
} from 'https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js';
import {
  initializeFirestore, persistentLocalCache, persistentMultipleTabManager,
  doc, getDoc, setDoc, deleteDoc, collection, onSnapshot, writeBatch, getDocs
} from 'https://www.gstatic.com/firebasejs/10.12.2/firebase-firestore.js';
import { firebaseConfig } from './firebase-config.js';

let A, auth, fs;
let user = null;
let phase = 'auth';           // auth -> loading -> ready
let ownerUnsubs = [];
let readerUnsubs = [];
let synced = null;            // what we believe the server already has (owner)
let pend = { s: 0, a: 0, f: 0 };
let rxUnsub = null, readingShare = null, readerRxSynced = null;

const OWNER_KEY = 'cassy-owner-uid';
const lsGetOwner = () => { try { return localStorage.getItem(OWNER_KEY); } catch (e) { return null; } };
const lsSetOwner = (v) => { try { localStorage.setItem(OWNER_KEY, v); } catch (e) {} };

/* ---------- helpers ---------- */
function canon(v) {
  if (v === undefined) return 'u';
  if (v === null || typeof v !== 'object') return JSON.stringify(v);
  if (Array.isArray(v)) return '[' + v.map(canon).join(',') + ']';
  return '{' + Object.keys(v).filter(k => v[k] !== undefined).sort().map(k => JSON.stringify(k) + ':' + canon(v[k])).join(',') + '}';
}
function clean(o) {
  const out = {};
  Object.keys(o).forEach(k => { if (o[k] !== undefined) out[k] = (o[k] && typeof o[k] === 'object' && !Array.isArray(o[k])) ? clean(o[k]) : o[k]; });
  return out;
}
async function sha(s) {
  const b = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(s));
  return Array.from(new Uint8Array(b)).map(x => x.toString(16).padStart(2, '0')).join('');
}
const shareIdFor = (pw) => sha('cassy-book|share|' + pw);
async function commitOps(ops) {
  for (let i = 0; i < ops.length; i += 400) {
    const b = writeBatch(fs);
    ops.slice(i, i + 400).forEach(fn => fn(b));
    await b.commit();
  }
}
function queueOps(ops) {
  // Offline-friendly: queue in local cache, don't wait for the server.
  for (let i = 0; i < ops.length; i += 400) {
    const b = writeBatch(fs);
    ops.slice(i, i + 400).forEach(fn => fn(b));
    b.commit().catch(e => A.toast('Couldn’t sync: ' + (e.code || e.message)));
  }
}
function answerFromDoc(d) {
  const a = { text: d.text || '', answeredAt: d.answeredAt || Date.now(), editedAt: d.editedAt || null };
  if (d.fav) a.fav = true;
  if (d.hidden) a.hidden = true;
  if (d.song && d.song.url) a.song = { url: d.song.url, title: d.song.title || '' };
  if (d.mood) a.mood = d.mood;
  if (d.lockUntil) a.lockUntil = d.lockUntil;
  if (Array.isArray(d.versions) && d.versions.length) a.versions = d.versions.map(v => ({ text: v.text || '', at: v.at || 0, mood: v.mood || null }));
  return a;
}
const isLocked = (a) => !!(a && a.lockUntil && a.lockUntil > Date.now());
function freeFromDoc(id, d) {
  const p = { id, title: d.title || '', text: d.text || '', answeredAt: d.answeredAt || Date.now(), editedAt: d.editedAt || null };
  if (d.fav) p.fav = true;
  if (d.hidden) p.hidden = true;
  return p;
}
function settingsOf(db) {
  return clean({
    ownerHash: db.ownerHash || null, readerOn: !!db.readerOn, readerPlain: db.readerPlain || null,
    readerHash: db.readerHash || null, shareId: db.shareId || null, cover: db.cover,
    skipped: db.skipped || {}, celebrated: db.celebrated || [], seen: db.seen || {}, shared: !!db.shared
  });
}
function buildMirror(db) {
  const m = {};
  Object.keys(db.answers).forEach(k => {
    const a = db.answers[k];
    if (a.hidden || isLocked(a) || !a.text.trim()) return;
    m['q' + k] = clean({ kind: 'q', n: +k, text: a.text, answeredAt: a.answeredAt, editedAt: a.editedAt || null, mood: a.mood || null, song: a.song });
  });
  db.free.forEach(p => {
    if (p.hidden || !(p.text || '').trim()) return;
    m['f' + p.id] = clean({ kind: 'f', id: p.id, title: p.title || '', text: p.text, answeredAt: p.answeredAt, editedAt: p.editedAt || null });
  });
  return m;
}
const canonMap = (m) => { const o = {}; Object.keys(m).forEach(k => { o[k] = canon(m[k]); }); return o; };

/* ---------- lifecycle ---------- */
export function init(api) {
  A = api;
  const app = initializeApp(firebaseConfig);
  auth = getAuth(app);
  fs = initializeFirestore(app, { localCache: persistentLocalCache({ tabManager: persistentMultipleTabManager() }) });
  getRedirectResult(auth).catch(() => {});
  onAuthStateChanged(auth, onUser);
}
export const busy = () => phase !== 'ready';
export const pending = () => pend.s + pend.a + pend.f;

function stopOwner() { ownerUnsubs.forEach(u => u()); ownerUnsubs = []; if (rxUnsub) { rxUnsub(); rxUnsub = null; } synced = null; pend = { s: 0, a: 0, f: 0 }; }
function stopReader() { readerUnsubs.forEach(u => u()); readerUnsubs = []; readingShare = null; readerRxSynced = null; }
function aggregateRx(snap) {
  const R = {};
  snap.docs.forEach(d => { const v = d.data() || {}, r = v.r || {}; Object.keys(r).forEach(key => { (R[key] = R[key] || {})[d.id] = { e: r[key].e, at: r[key].at || 0, name: v.name || 'Someone' }; }); });
  return R;
}
function listenOwnerRx(sid) {
  if (rxUnsub) { rxUnsub(); rxUnsub = null; }
  if (!sid) { A.getDb().reactions = {}; return; }
  rxUnsub = onSnapshot(collection(fs, 'shared', sid, 'reactions'), (snap) => {
    A.getDb().reactions = aggregateRx(snap);
    if (phase === 'ready') { A.softRender(); if (A.checkStickers) A.checkStickers(); }
  }, () => {});
}

async function onUser(u) {
  stopOwner(); stopReader();
  user = u; A.S.unlocked = false;
  if (!u) {
    A.dev.account = null; A.saveDev(); A.setDb(A.freshDb(false));
    phase = 'ready'; A.start(); return;
  }
  phase = 'loading'; A.start();
  let role;
  try { role = await resolveRole(u); }
  catch (e) { const c = lsGetOwner(); role = c && c === u.uid ? 'owner' : 'reader'; }
  A.dev.account = { role, name: u.displayName || 'You', email: u.email || '', uid: u.uid };
  A.saveDev();
  A.setDb(A.freshDb(false));
  if (role === 'owner') {
    try { await loadOwner(); } catch (e) { A.toast('Couldn’t load your book: ' + (e.code || e.message)); }
  }
  phase = 'ready'; A.start();
}
async function resolveRole(u) {
  const ref = doc(fs, 'app', 'owner');
  const s = await getDoc(ref);
  if (!s.exists()) {
    await setDoc(ref, { uid: u.uid, name: u.displayName || '', createdAt: Date.now() });
    lsSetOwner(u.uid); return 'owner';
  }
  const ownerUid = s.data().uid; lsSetOwner(ownerUid);
  return ownerUid === u.uid ? 'owner' : 'reader';
}

/* ---------- owner ---------- */
function loadOwner() {
  return new Promise((resolve, reject) => {
    const uid = user.uid;
    const got = { s: false, a: false, f: false };
    synced = { settings: null, answers: {}, free: {}, mirror: {}, cover: null };
    let resolved = false;
    const ready = () => {
      if (resolved || !(got.s && got.a && got.f)) return;
      resolved = true;
      const db = A.getDb();
      if (db.readerOn && db.shareId) { synced.mirror = canonMap(buildMirror(db)); synced.cover = canon(db.cover); listenOwnerRx(db.shareId); }
      resolve();
      setTimeout(() => sync(), 50); // shares any time capsule that opened since the last visit
    };
    const fail = (e) => { if (!resolved) { resolved = true; reject(e); } else A.toast('Sync problem: ' + (e.code || e.message)); };

    ownerUnsubs.push(onSnapshot(doc(fs, 'users', uid), { includeMetadataChanges: true }, (s) => {
      pend.s = s.metadata.hasPendingWrites ? 1 : 0;
      if (!got.s || !s.metadata.hasPendingWrites) {
        const d = s.data() || {}, db = A.getDb();
        db.ownerHash = d.ownerHash || null; db.readerOn = !!d.readerOn; db.readerPlain = d.readerPlain || null;
        db.readerHash = d.readerHash || null; db.shareId = d.shareId || null;
        if (d.cover) db.cover = d.cover;
        const oldShare = db.shareId;
        db.skipped = d.skipped || {}; db.celebrated = d.celebrated || []; db.seen = d.seen || db.seen || { stickers: null, rx: 0 }; db.shared = !!d.shared;
        if (got.s && db.shareId !== oldShare) listenOwnerRx(db.readerOn ? db.shareId : null);
        const c = canon(settingsOf(db));
        const changed = got.s && synced.settings !== c;
        synced.settings = s.exists() ? c : null;
        got.s = true; ready();
        if (changed && phase === 'ready') A.softRender();
      }
      A.statusRefresh();
    }, fail));

    ownerUnsubs.push(onSnapshot(collection(fs, 'users', uid, 'answers'), { includeMetadataChanges: true }, (snap) => {
      const db = A.getDb(); let changed = false;
      snap.docChanges().forEach(ch => {
        const n = +ch.doc.id;
        if (ch.type === 'removed') {
          if (!ch.doc.metadata.hasPendingWrites && db.answers[n]) { delete db.answers[n]; changed = true; }
          delete synced.answers[n]; return;
        }
        if (got.a && ch.doc.metadata.hasPendingWrites) return; // our own local echo
        const a = answerFromDoc(ch.doc.data()), c = canon(a);
        if (canon(db.answers[n]) !== c) { db.answers[n] = a; changed = true; }
        synced.answers[n] = c;
      });
      pend.a = snap.docs.filter(d => d.metadata.hasPendingWrites).length;
      got.a = true; ready();
      if (changed && phase === 'ready') A.softRender();
      A.statusRefresh();
    }, fail));

    ownerUnsubs.push(onSnapshot(collection(fs, 'users', uid, 'free'), { includeMetadataChanges: true }, (snap) => {
      const db = A.getDb(); let changed = false;
      snap.docChanges().forEach(ch => {
        const id = ch.doc.id;
        if (ch.type === 'removed') {
          if (!ch.doc.metadata.hasPendingWrites && db.free.some(p => p.id === id)) { db.free = db.free.filter(p => p.id !== id); changed = true; }
          delete synced.free[id]; return;
        }
        if (got.f && ch.doc.metadata.hasPendingWrites) return;
        const p = freeFromDoc(id, ch.doc.data()), c = canon(p);
        const i = db.free.findIndex(x => x.id === id);
        if (i < 0) { db.free.push(p); changed = true; }
        else if (canon(db.free[i]) !== c) { db.free[i] = p; changed = true; }
        synced.free[id] = c;
      });
      db.free.sort((x, y) => x.answeredAt - y.answeredAt);
      pend.f = snap.docs.filter(d => d.metadata.hasPendingWrites).length;
      got.f = true; ready();
      if (changed && phase === 'ready') A.softRender();
      A.statusRefresh();
    }, fail));
  });
}

// Called by the UI after every change. Writes only what differs from the server copy.
export function sync() {
  if (user && readingShare && A.dev.account && A.dev.account.role === 'reader') { syncReaderRx(); return; }
  if (!user || !synced || !A.dev.account || A.dev.account.role !== 'owner') return;
  const db = A.getDb(), uid = user.uid, ops = [];
  const st = settingsOf(db), stc = canon(st);
  if (stc !== synced.settings) { ops.push(b => b.set(doc(fs, 'users', uid), st)); synced.settings = stc; }

  Object.keys(db.answers).forEach(k => {
    const a = db.answers[k], c = canon(a);
    if (c !== synced.answers[k]) { const data = clean(Object.assign({ n: +k }, a)); ops.push(b => b.set(doc(fs, 'users', uid, 'answers', String(k)), data)); synced.answers[k] = c; }
  });
  Object.keys(synced.answers).forEach(k => {
    if (!db.answers[k]) { ops.push(b => b.delete(doc(fs, 'users', uid, 'answers', String(k)))); delete synced.answers[k]; }
  });

  const ids = {};
  db.free.forEach(p => {
    ids[p.id] = 1; const c = canon(p);
    if (c !== synced.free[p.id]) { const data = clean(Object.assign({}, p)); ops.push(b => b.set(doc(fs, 'users', uid, 'free', p.id), data)); synced.free[p.id] = c; }
  });
  Object.keys(synced.free).forEach(id => {
    if (!ids[id]) { ops.push(b => b.delete(doc(fs, 'users', uid, 'free', id))); delete synced.free[id]; }
  });

  if (db.readerOn && db.shareId) {
    const sid = db.shareId, m = buildMirror(db), prev = synced.mirror || {}, next = {};
    Object.keys(m).forEach(k => {
      const c = canon(m[k]); next[k] = c;
      if (c !== prev[k]) ops.push(b => b.set(doc(fs, 'shared', sid, 'pages', k), m[k]));
    });
    Object.keys(prev).forEach(k => { if (!next[k]) ops.push(b => b.delete(doc(fs, 'shared', sid, 'pages', k))); });
    synced.mirror = next;
    const cc = canon(db.cover);
    if (cc !== synced.cover) { const cover = db.cover; ops.push(b => b.set(doc(fs, 'shared', sid), { owner: uid, cover, updatedAt: Date.now() })); synced.cover = cc; }
  }
  if (ops.length) queueOps(ops);
}

function syncReaderRx() {
  const db = A.getDb(), me = user.uid, R = db.reactions || {}, mine = {};
  Object.keys(R).forEach(key => { if (R[key][me]) mine[key] = { e: R[key][me].e, at: R[key][me].at || Date.now() }; });
  const c = canon(mine);
  if (c === readerRxSynced) return;
  readerRxSynced = c;
  const name = ((A.dev.account && A.dev.account.name) || 'Someone').split(' ')[0];
  setDoc(doc(fs, 'shared', readingShare, 'reactions', me), { name, r: mine, updatedAt: Date.now() }).catch(e => A.toast('Couldn\u2019t save your reaction: ' + (e.code || e.message)));
}

async function deleteShared(id) {
  for (const sub of ['pages', 'reactions']) {
    const snap = await getDocs(collection(fs, 'shared', id, sub));
    await commitOps(snap.docs.map(d => b => b.delete(d.ref)));
  }
  await deleteDoc(doc(fs, 'shared', id));
}

export async function changeReader(pw) {
  if (!navigator.onLine) throw new Error('You need internet to set the reader password.');
  const db = A.getDb(), uid = user.uid, id = await shareIdFor(pw), old = db.shareId;
  try {
    await setDoc(doc(fs, 'shared', id), { owner: uid, cover: db.cover, updatedAt: Date.now() });
  } catch (e) {
    throw new Error('That reader password can’t be used. Please choose a different one.');
  }
  const m = buildMirror(db);
  await commitOps(Object.keys(m).map(k => b => b.set(doc(fs, 'shared', id, 'pages', k), m[k])));
  if (old && old !== id) {
    try {
      const rs = await getDocs(collection(fs, 'shared', old, 'reactions'));
      await commitOps(rs.docs.map(d => b => b.set(doc(fs, 'shared', id, 'reactions', d.id), d.data())));
    } catch (e) {}
    try { await deleteShared(old); } catch (e) {}
  }
  db.shareId = id; db.readerPlain = pw; db.readerHash = await A.hash(pw); db.readerOn = true;
  synced.mirror = canonMap(m); synced.cover = canon(db.cover);
  listenOwnerRx(id);
  sync();
}

export async function setReaderOn(on) {
  if (!navigator.onLine) throw new Error('You need internet for this.');
  const db = A.getDb();
  if (!on) {
    if (db.shareId) await deleteShared(db.shareId);
    db.readerOn = false; synced.mirror = {}; synced.cover = null; listenOwnerRx(null); sync(); return;
  }
  if (!db.readerPlain) throw new Error('Set a reader password first.');
  await changeReader(db.readerPlain);
}

/* ---------- reader ---------- */
export async function readerUnlock(pw) {
  const id = await shareIdFor(pw);
  let s;
  try { s = await getDoc(doc(fs, 'shared', id)); }
  catch (e) { return { ok: false, msg: navigator.onLine ? 'Couldn’t check the password. Try again.' : 'Connect to the internet to unlock.' }; }
  if (!s.exists()) return { ok: false, msg: 'Wrong password, or reading is turned off.' };
  await listenShared(id);
  readingShare = id;
  return { ok: true };
}
function listenShared(id) {
  stopReader();
  return new Promise((resolve) => {
    let first = true;
    readerUnsubs.push(onSnapshot(doc(fs, 'shared', id), (s) => {
      if (!s.exists()) {
        if (A.S.unlocked) { stopReader(); A.S.unlocked = false; A.toast('The reader password changed or reading was turned off.'); A.start(); }
        return;
      }
      const d = s.data(); if (d.cover) { A.getDb().cover = d.cover; if (!first) A.softRender(); }
    }, () => {}));
    readerUnsubs.push(onSnapshot(collection(fs, 'shared', id, 'pages'), (snap) => {
      const db = A.getDb(); db.answers = {}; db.free = [];
      snap.docs.forEach(d => {
        const p = d.data();
        if (p.kind === 'q') db.answers[p.n] = answerFromDoc(p);
        else if (p.kind === 'f') db.free.push(freeFromDoc(p.id, p));
      });
      db.free.sort((x, y) => x.answeredAt - y.answeredAt);
      if (first) { first = false; resolve(); } else A.softRender();
    }, () => { if (first) { first = false; resolve(); } }));
    readerUnsubs.push(onSnapshot(collection(fs, 'shared', id, 'reactions'), (snap) => {
      const db = A.getDb(), me = user.uid;
      db.reactions = aggregateRx(snap);
      const mine = {}; Object.keys(db.reactions).forEach(k => { if (db.reactions[k][me]) mine[k] = { e: db.reactions[k][me].e, at: db.reactions[k][me].at }; });
      readerRxSynced = canon(mine);
      if (!first) A.softRender();
    }, () => {}));
  });
}

/* ---------- auth ---------- */
export async function signIn() {
  const p = new GoogleAuthProvider();
  p.setCustomParameters({ prompt: 'select_account' });
  try { await signInWithPopup(auth, p); }
  catch (e) {
    const redirect = ['auth/popup-blocked', 'auth/operation-not-supported-in-this-environment', 'auth/web-storage-unsupported'];
    if (redirect.indexOf(e.code) >= 0) { await signInWithRedirect(auth, p); return; }
    if (e.code !== 'auth/popup-closed-by-user' && e.code !== 'auth/cancelled-popup-request') A.toast('Sign-in failed: ' + (e.code || e.message));
  }
}
export async function signOut() {
  stopOwner(); stopReader();
  await fbSignOut(auth);
}
