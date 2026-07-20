# LakeSense marketing site

Vite + React + **React Three Fiber**. Art direction: *abyssal bioluminescence* —
a data lake sensing its own depth. The hero is a generated WebGL scene (a
particle field spiraling into a crystalline "lake"); Three.js is code-split and
loads after first paint over a static poster.

## Develop

```bash
npm install
npm run dev      # http://localhost:5173
npm run build    # static output in dist/
npm run preview  # serve the build
```

Deploy `dist/` to any static host (GitHub Pages / Netlify / Vercel).

## Graceful degradation (built in)

The live WebGL scene is skipped — and the static bioluminescent poster shown
instead — when any of these is true (`src/scene/capabilities.ts`):

- `prefers-reduced-motion: reduce`
- WebGL is unavailable
- viewport width < 768px (mobile)
- the URL carries `?still` (for captures/print)

The main bundle stays light (~65 kB gzipped); the Three.js chunk (~235 kB
gzipped) is fetched lazily and only when the scene will actually render.

## Hero video (optional)

The hero supports a `<video>` background instead of the WebGL scene. It ships
**without** a video file — drop a royalty-free clip at `public/hero.mp4` (check
the clip's license first; e.g. Pexels/Coverr) and wire it into the hero
background in `src/App.tsx`. We never bundle or hotlink someone else's footage.

## Screenshots

Product screenshots in `public/shots/` are real captures of the dashboard,
used in the "product" showcase section.
