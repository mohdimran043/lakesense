// Client-side settings persisted in localStorage. The cost model feeds the
// Analytics view's transparent estimate ($/GB stored + $/compute-hour) — the
// same knobs the backend /analytics endpoint accepts as query params.

export interface CostModel {
  costPerGB: number;
  costPerHour: number;
}

const KEY = "lakesense.costModel";
export const defaultCostModel: CostModel = { costPerGB: 0.023, costPerHour: 0.1 };

export function getCostModel(): CostModel {
  try {
    const raw = localStorage.getItem(KEY);
    if (!raw) return defaultCostModel;
    const parsed = JSON.parse(raw) as Partial<CostModel>;
    return {
      costPerGB: Number.isFinite(parsed.costPerGB) ? Number(parsed.costPerGB) : defaultCostModel.costPerGB,
      costPerHour: Number.isFinite(parsed.costPerHour) ? Number(parsed.costPerHour) : defaultCostModel.costPerHour,
    };
  } catch {
    return defaultCostModel;
  }
}

export function setCostModel(m: CostModel): void {
  localStorage.setItem(KEY, JSON.stringify(m));
}
