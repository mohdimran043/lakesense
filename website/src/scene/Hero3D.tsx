import { useMemo, useRef } from "react";
import { Canvas, useFrame } from "@react-three/fiber";
import * as THREE from "three";

// The signature scene: a bioluminescent particle field spiraling inward — data
// streaming from the edges into a crystalline "lake" at the centre, which the
// platform continuously senses. Aqua (the brand signal) + a violet depth glow.

const COUNT = 1600;
const OUTER = 9;
const INNER = 1.1;

function DataStream() {
  const pointsRef = useRef<THREE.Points>(null);
  const { positions, speeds } = useMemo(() => {
    const positions = new Float32Array(COUNT * 3);
    const speeds = new Float32Array(COUNT);
    for (let i = 0; i < COUNT; i++) spawn(positions, i);
    for (let i = 0; i < COUNT; i++) speeds[i] = 0.004 + Math.random() * 0.01;
    return { positions, speeds };
  }, []);

  useFrame((_, delta) => {
    const pts = pointsRef.current;
    if (!pts) return;
    const arr = (pts.geometry.attributes.position as THREE.BufferAttribute).array as Float32Array;
    const k = Math.min(delta, 0.05) * 60; // frame-rate independent
    for (let i = 0; i < COUNT; i++) {
      const ix = i * 3;
      let x = arr[ix];
      let z = arr[ix + 2];
      const r = Math.hypot(x, z);
      if (r < INNER) {
        spawn(arr, i); // absorbed by the lake → respawn at the edge
        continue;
      }
      // Spiral inward: move toward centre + a tangential swirl.
      const pull = speeds[i] * k;
      const ang = Math.atan2(z, x) + 0.012 * k;
      const nr = r * (1 - pull);
      arr[ix] = Math.cos(ang) * nr;
      arr[ix + 2] = Math.sin(ang) * nr;
      arr[ix + 1] *= 1 - pull * 0.4; // ease toward the surface plane
    }
    (pts.geometry.attributes.position as THREE.BufferAttribute).needsUpdate = true;
    pts.rotation.y += delta * 0.03;
  });

  return (
    <points ref={pointsRef}>
      <bufferGeometry>
        <bufferAttribute attach="attributes-position" args={[positions, 3]} />
      </bufferGeometry>
      <pointsMaterial
        size={0.05}
        color="#2EE6D6"
        transparent
        opacity={0.85}
        sizeAttenuation
        blending={THREE.AdditiveBlending}
        depthWrite={false}
      />
    </points>
  );
}

function spawn(arr: Float32Array, i: number) {
  const ang = Math.random() * Math.PI * 2;
  const r = INNER + 2 + Math.random() * (OUTER - INNER - 2);
  const ix = i * 3;
  arr[ix] = Math.cos(ang) * r;
  arr[ix + 1] = (Math.random() - 0.5) * 2.4;
  arr[ix + 2] = Math.sin(ang) * r;
}

function Crystal() {
  const ref = useRef<THREE.Group>(null);
  useFrame((state, delta) => {
    if (!ref.current) return;
    ref.current.rotation.y += delta * 0.15;
    ref.current.rotation.x = Math.sin(state.clock.elapsedTime * 0.2) * 0.15;
  });
  return (
    <group ref={ref}>
      <mesh>
        <icosahedronGeometry args={[1, 0]} />
        <meshStandardMaterial color="#0B1220" emissive="#2EE6D6" emissiveIntensity={0.25} roughness={0.2} metalness={0.6} />
      </mesh>
      <mesh scale={1.02}>
        <icosahedronGeometry args={[1, 0]} />
        <meshBasicMaterial color="#2EE6D6" wireframe transparent opacity={0.5} />
      </mesh>
    </group>
  );
}

function CameraDrift() {
  useFrame((state) => {
    const t = state.clock.elapsedTime;
    state.camera.position.x = Math.sin(t * 0.1) * 0.6;
    state.camera.position.y = Math.cos(t * 0.13) * 0.35 + 0.4;
    state.camera.lookAt(0, 0, 0);
  });
  return null;
}

export function Hero3D() {
  return (
    <Canvas
      dpr={[1, 1.5]}
      camera={{ position: [0, 0.4, 7], fov: 55 }}
      gl={{ antialias: true, powerPreference: "high-performance" }}
    >
      <ambientLight intensity={0.4} />
      <pointLight position={[5, 3, 4]} color="#2EE6D6" intensity={40} distance={20} />
      <pointLight position={[-5, -2, -3]} color="#8B7BFF" intensity={35} distance={20} />
      <Crystal />
      <DataStream />
      <CameraDrift />
      <fog attach="fog" args={["#060A12", 8, 16]} />
    </Canvas>
  );
}
