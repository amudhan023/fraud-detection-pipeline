export type Classification =
  | "CONFIRMED_FRAUD"
  | "LIKELY_FRAUD"
  | "SUSPICIOUS"
  | "LEGITIMATE"
  | "UNKNOWN";

export type ActionType = "APPROVE" | "DENY" | "ESCALATE" | "BLOCK_ACCOUNT";

export interface AIFraudAnalysis {
  id: string;
  anomaly_event_id: string;
  is_fraud: boolean;
  confidence: number;
  ai_risk_score: number;
  classification: Classification;
  reasoning: string;
  recommended_actions: string[];
  model_used: string;
  analyzed_at: string;
}

export interface CaseAction {
  id: string;
  anomaly_event_id: string;
  action: ActionType;
  notes: string | null;
  performed_at: string;
}

export interface FraudEvent {
  id: string;
  transaction_id: string;
  account_id: string;
  reason_code: string;
  evidence: Record<string, unknown> | null;
  detected_at: string;
  amount: number | null;
  currency: string | null;
  latitude: number | null;
  longitude: number | null;
  event_time: string | null;
  rule_risk_score: number | null;
  score_breakdown: Record<string, unknown> | null;
  owner_name: string | null;
  tier: string | null;
  account_country: string | null;
  credit_limit: number | null;
  account_risk_score: number | null;
  merchant_name: string | null;
  merchant_category: string | null;
  merchant_risk_category: string | null;
  merchant_country: string | null;
  ai_analysis: AIFraudAnalysis | null;
  last_action: string | null;
  case_actions: CaseAction[];
}

export interface DashboardStats {
  total_events: number;
  high_risk_count: number;
  pending_review_count: number;
  confirmed_fraud_count: number;
  analyzed_count: number;
}

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API ${path} → ${res.status}: ${text}`);
  }
  return res.json() as Promise<T>;
}

export const api = {
  stats: () => apiFetch<DashboardStats>("/api/stats"),

  events: (limit = 100, offset = 0) =>
    apiFetch<FraudEvent[]>(`/api/fraud-events?limit=${limit}&offset=${offset}`),

  event: (id: string) => apiFetch<FraudEvent>(`/api/fraud-events/${id}`),

  analyze: (id: string) =>
    apiFetch<{ status: string; analysis: AIFraudAnalysis | null }>(
      `/api/fraud-events/${id}/analyze`,
      { method: "POST" }
    ),

  recordAction: (id: string, action: ActionType, notes?: string) =>
    apiFetch(`/api/fraud-events/${id}/action`, {
      method: "POST",
      body: JSON.stringify({ action, notes }),
    }),
};
