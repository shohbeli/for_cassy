# for_cassy

**1000 Things to Write About Cassy**: a private, installable web app (PWA) with 1000 questions.

- Owner signs in with Google, then enters their own password every time the app opens.
- Readers sign in with Google, then enter the reader password. Read-only.
- Works offline. Answers save on the phone and sync to Firebase when back online.

## Files
- `index.html`: the app (UI, styles, and the 1000 questions)
- `live.js`: Firebase sign-in, database, offline sync, read-only sharing
- `firebase-config.js`: public Firebase project settings
- `sw.js`, `manifest.webmanifest`, `icons/`: offline support and home-screen install
- `firestore.rules`: database security rules (paste into Firebase → Firestore → Rules)

## Live link
https://shohbeli.github.io/for_cassy/
