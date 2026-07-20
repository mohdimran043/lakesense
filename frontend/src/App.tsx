import { Navigate, Route, Routes } from "react-router-dom";
import { AppShell } from "./components/AppShell";
import { Dashboard } from "./pages/Dashboard";
import { Pipelines } from "./pages/Pipelines";
import { PipelineDetail } from "./pages/PipelineDetail";
import { Incidents } from "./pages/Incidents";
import { Diff } from "./pages/Diff";
import { Analytics } from "./pages/Analytics";
import { Audit } from "./pages/Audit";

export default function App() {
  return (
    <AppShell>
      <Routes>
        <Route path="/" element={<Dashboard />} />
        <Route path="/pipelines" element={<Pipelines />} />
        <Route path="/pipelines/:id" element={<PipelineDetail />} />
        <Route path="/incidents" element={<Incidents />} />
        <Route path="/diff" element={<Diff />} />
        <Route path="/analytics" element={<Analytics />} />
        <Route path="/audit" element={<Audit />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </AppShell>
  );
}
