"use client";

import type { DashboardStats } from "@/lib/api";

interface StatCardProps {
  label: string;
  value: number | string;
  sub?: string;
  accent?: string;
}

function StatCard({ label, value, sub, accent = "#60a5fa" }: StatCardProps) {
  return (
    <div
      className="rounded-xl border p-5 flex flex-col gap-1"
      style={{ background: "var(--surface)", borderColor: "var(--border)" }}
    >
      <p className="text-xs uppercase tracking-wider" style={{ color: "var(--text-muted)" }}>
        {label}
      </p>
      <p className="text-3xl font-bold" style={{ color: accent }}>
        {value}
      </p>
      {sub && (
        <p className="text-xs" style={{ color: "var(--text-muted)" }}>
          {sub}
        </p>
      )}
    </div>
  );
}

export function StatsCards({ stats }: { stats: DashboardStats }) {
  const pct =
    stats.total_events > 0
      ? Math.round((stats.analyzed_count / stats.total_events) * 100)
      : 0;

  return (
    <div className="grid grid-cols-2 lg:grid-cols-5 gap-4 mb-6">
      <StatCard label="Total Events" value={stats.total_events} accent="#60a5fa" />
      <StatCard
        label="High Risk"
        value={stats.high_risk_count}
        sub="AI score ≥ 70"
        accent="#f97316"
      />
      <StatCard
        label="Pending Review"
        value={stats.pending_review_count}
        sub="Unanalyzed"
        accent="#facc15"
      />
      <StatCard
        label="Confirmed Fraud"
        value={stats.confirmed_fraud_count}
        accent="#ef4444"
      />
      <StatCard
        label="Analyzed"
        value={`${pct}%`}
        sub={`${stats.analyzed_count} / ${stats.total_events}`}
        accent="#34d399"
      />
    </div>
  );
}
