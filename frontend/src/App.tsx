import { Navigate, Route, Routes } from "react-router-dom";
import { AppShell } from "./components/AppShell";
import { Dashboard } from "./pages/Dashboard";
import { Pipelines } from "./pages/Pipelines";
import { CreatePipeline } from "./pages/CreatePipeline";
import { PipelineDetail } from "./pages/PipelineDetail";
import { Incidents } from "./pages/Incidents";
import { Alerts } from "./pages/Alerts";
import { Diff } from "./pages/Diff";
import { Analytics } from "./pages/Analytics";
import { Audit } from "./pages/Audit";
import { Escalations } from "./pages/Escalations";
import { Settings } from "./pages/Settings";

export default function App() {
  return (
    <AppShell>
      <Routes>
        <Route path="/" element={<Dashboard />} />
        <Route path="/pipelines" element={<Pipelines />} />
        <Route path="/pipelines/new" element={<CreatePipeline />} />
        <Route path="/pipelines/:id" element={<PipelineDetail />} />
        <Route path="/incidents" element={<Incidents />} />
        <Route path="/alerts" element={<Alerts />} />
        <Route path="/diff" element={<Diff />} />
        <Route path="/escalations" element={<Escalations />} />
        <Route path="/analytics" element={<Analytics />} />
        <Route path="/audit" element={<Audit />} />
        <Route path="/settings" element={<Settings />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </AppShell>
  );
}
