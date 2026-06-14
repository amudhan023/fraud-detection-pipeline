"use client";

import { useState } from "react";
import { format } from "date-fns";
import type { ActionType, FraudEvent } from "@/lib/api";
import { api } from "@/lib/api";
import { AnomalyBadge, ClassificationBadge, ScoreBadge } from "./RiskBadge";

interface Props {
  event: FraudEvent | null;
  onClose: () => void;
  onActionRecorded: () => void;
}

const ACTIONS: { type: ActionType; label: string; cls: string }[] = [
  { type: "APPROVE", label: "Approve", cls: "bg-green-700 hover:bg-green-600 text-white" },
  { type: "DENY", label: "Deny", cls: "bg-red-700 hover:bg-red-600 text-white" },
  { type: "ESCALATE", label: "Escalate", cls: "bg-yellow-700 hover:bg-yellow-600 text-white" },
  { type: "BLOCK_ACCOUNT", label: "Block Account", cls: "bg-red-900 hover:bg-red-800 text-red-200 border border-red-700" },
];

export function EventDetailPanel({ event, onClose, onActionRecorded }: Props) {
  const [analyzing, setAnalyzing] = useState(false);
  const [actioning, setActioning] = useState<ActionType | null>(null);
  const [error, setError] = useState<string | null>(null);

  if (!event) return null;

  const ai = event.ai_analysis;
  const ruleScore = event.rule_risk_score != null ? Math.round(event.rule_risk_score * 100) : null;

  async function triggerAnalysis() {
    if (!event) return;
    setAnalyzing(true);
    setError(null);
    try {
      await api.analyze(event.id);
      onActionRecorded();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Analysis failed");
    } finally {
      setAnalyzing(false);
    }
  }

  async function takeAction(action: ActionType) {
    if (!event) return;
    setActioning(action);
    setError(null);
    try {
      await api.recordAction(event.id, action);
      onActionRecorded();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Action failed");
    } finally {
      setActioning(null);
    }
  }

  return (
    <div
      className="fixed inset-y-0 right-0 w-full sm:w-[520px] shadow-2xl border-l flex flex-col z-40 overflow-y-auto"
      style={{ background: "var(--surface)", borderColor: "var(--border)" }}
    >
      {/* Header */}
      <div
        className="sticky top-0 flex items-center justify-between px-5 py-4 border-b"
        style={{ background: "var(--surface)", borderColor: "var(--border)" }}
      >
        <div className="flex items-center gap-2">
          <AnomalyBadge code={event.reason_code} />
          <span className="text-sm font-mono text-slate-400 truncate max-w-[160px]">
            {event.transaction_id.slice(0, 8)}…
          </span>
        </div>
        <button
          onClick={onClose}
          className="text-slate-400 hover:text-white transition-colors text-xl leading-none"
        >
          ×
        </button>
      </div>

      <div className="flex-1 px-5 py-4 space-y-5">
        {/* AI Analysis block */}
        <section>
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-xs font-semibold uppercase tracking-wider text-slate-400">
              AI Analysis
            </h2>
            {!ai && (
              <button
                onClick={triggerAnalysis}
                disabled={analyzing}
                className="text-xs px-3 py-1 rounded-md bg-blue-700 hover:bg-blue-600 text-white disabled:opacity-50 transition-colors"
              >
                {analyzing ? "Analyzing…" : "Run Analysis"}
              </button>
            )}
          </div>

          {ai ? (
            <div
              className="rounded-lg p-4 space-y-3 border"
              style={{ background: "var(--surface-2)", borderColor: "var(--border)" }}
            >
              <div className="flex items-center justify-between">
                <ClassificationBadge classification={ai.classification} />
                <div className="flex items-center gap-3">
                  <div className="text-right">
                    <div className="text-xs text-slate-500">AI Score</div>
                    <ScoreBadge score={ai.ai_risk_score} />
                  </div>
                  <div className="text-right">
                    <div className="text-xs text-slate-500">Confidence</div>
                    <div className="text-sm font-semibold text-slate-300">
                      {Math.round(ai.confidence * 100)}%
                    </div>
                  </div>
                </div>
              </div>

              <div>
                <div className="text-xs text-slate-500 mb-1">Reasoning</div>
                <p className="text-sm text-slate-300 leading-relaxed">{ai.reasoning}</p>
              </div>

              {ai.recommended_actions.length > 0 && (
                <div>
                  <div className="text-xs text-slate-500 mb-1">Recommended Actions</div>
                  <ul className="space-y-1">
                    {ai.recommended_actions.map((a, i) => (
                      <li key={i} className="text-xs font-mono text-slate-300 flex items-center gap-1">
                        <span className="text-blue-400">›</span> {a}
                      </li>
                    ))}
                  </ul>
                </div>
              )}

              <div className="text-xs text-slate-600">
                {ai.model_used} · {format(new Date(ai.analyzed_at), "MMM d, HH:mm:ss")}
              </div>
            </div>
          ) : (
            <div
              className="rounded-lg p-4 text-center border"
              style={{ background: "var(--surface-2)", borderColor: "var(--border)" }}
            >
              {analyzing ? (
                <p className="text-sm text-slate-400">Claude is analyzing this event…</p>
              ) : (
                <p className="text-sm text-slate-500">
                  No AI analysis yet. Click "Run Analysis" to classify this event.
                </p>
              )}
            </div>
          )}
        </section>

        {/* Action buttons */}
        <section>
          <h2 className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-3">
            Take Action
          </h2>
          <div className="grid grid-cols-2 gap-2">
            {ACTIONS.map((a) => (
              <button
                key={a.type}
                onClick={() => takeAction(a.type)}
                disabled={actioning !== null}
                className={`text-sm font-medium px-3 py-2 rounded-lg transition-colors disabled:opacity-50 ${a.cls}`}
              >
                {actioning === a.type ? "…" : a.label}
              </button>
            ))}
          </div>
          {event.last_action && (
            <p className="text-xs text-slate-500 mt-2">
              Last action: <span className="text-slate-300 font-mono">{event.last_action}</span>
            </p>
          )}
        </section>

        {error && (
          <p className="text-xs text-red-400 bg-red-900/20 rounded px-3 py-2 border border-red-800">
            {error}
          </p>
        )}

        {/* Transaction details */}
        <section>
          <h2 className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-3">
            Transaction
          </h2>
          <Grid>
            <KV k="Amount">
              {event.amount != null
                ? `$${event.amount.toLocaleString("en-US", { minimumFractionDigits: 2 })} ${event.currency}`
                : "—"}
            </KV>
            <KV k="Rule Score"><ScoreBadge score={ruleScore} /></KV>
            <KV k="Time">{event.event_time ? format(new Date(event.event_time), "PPpp") : "—"}</KV>
            <KV k="Location">
              {event.latitude != null && event.longitude != null
                ? `${event.latitude.toFixed(4)}, ${event.longitude.toFixed(4)}`
                : "—"}
            </KV>
          </Grid>
        </section>

        {/* Account details */}
        <section>
          <h2 className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-3">
            Account
          </h2>
          <Grid>
            <KV k="Owner">{event.owner_name ?? "—"}</KV>
            <KV k="Tier">{event.tier ?? "—"}</KV>
            <KV k="Country">{event.account_country ?? "—"}</KV>
            <KV k="Credit Limit">
              {event.credit_limit != null
                ? `$${event.credit_limit.toLocaleString("en-US", { minimumFractionDigits: 2 })}`
                : "—"}
            </KV>
            <KV k="Risk Score">
              {event.account_risk_score != null ? event.account_risk_score.toFixed(2) : "—"}
            </KV>
          </Grid>
        </section>

        {/* Merchant details */}
        <section>
          <h2 className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-3">
            Merchant
          </h2>
          <Grid>
            <KV k="Name">{event.merchant_name ?? "—"}</KV>
            <KV k="Category">{event.merchant_category ?? "—"}</KV>
            <KV k="Risk">{event.merchant_risk_category ?? "—"}</KV>
            <KV k="Country">{event.merchant_country ?? "—"}</KV>
          </Grid>
        </section>

        {/* Evidence */}
        {event.evidence && (
          <section>
            <h2 className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-3">
              Anomaly Evidence
            </h2>
            <pre
              className="text-xs font-mono p-3 rounded-lg overflow-x-auto border"
              style={{
                background: "var(--surface-2)",
                borderColor: "var(--border)",
                color: "var(--text-muted)",
              }}
            >
              {JSON.stringify(event.evidence, null, 2)}
            </pre>
          </section>
        )}

        {/* Action history */}
        {event.case_actions && event.case_actions.length > 0 && (
          <section>
            <h2 className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-3">
              Action History
            </h2>
            <ul className="space-y-2">
              {event.case_actions.map((ca) => (
                <li
                  key={ca.id}
                  className="text-xs flex items-start justify-between border rounded-md px-3 py-2"
                  style={{ borderColor: "var(--border)", background: "var(--surface-2)" }}
                >
                  <div>
                    <span className="font-mono text-slate-300">{ca.action}</span>
                    {ca.notes && (
                      <p className="text-slate-500 mt-0.5">{ca.notes}</p>
                    )}
                  </div>
                  <span className="text-slate-600 whitespace-nowrap ml-4">
                    {format(new Date(ca.performed_at), "HH:mm:ss")}
                  </span>
                </li>
              ))}
            </ul>
          </section>
        )}
      </div>
    </div>
  );
}

function Grid({ children }: { children: React.ReactNode }) {
  return <dl className="grid grid-cols-2 gap-x-4 gap-y-2">{children}</dl>;
}

function KV({ k, children }: { k: string; children: React.ReactNode }) {
  return (
    <div>
      <dt className="text-xs text-slate-500">{k}</dt>
      <dd className="text-sm text-slate-200 mt-0.5">{children}</dd>
    </div>
  );
}
