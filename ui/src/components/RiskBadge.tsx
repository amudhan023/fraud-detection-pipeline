"use client";

import type { Classification } from "@/lib/api";
import clsx from "clsx";

const classConfig: Record<
  Classification | "UNANALYZED",
  { label: string; cls: string }
> = {
  CONFIRMED_FRAUD: { label: "Confirmed Fraud", cls: "bg-red-900 text-red-300 border border-red-700" },
  LIKELY_FRAUD:    { label: "Likely Fraud",    cls: "bg-orange-900 text-orange-300 border border-orange-700" },
  SUSPICIOUS:      { label: "Suspicious",      cls: "bg-yellow-900 text-yellow-300 border border-yellow-700" },
  LEGITIMATE:      { label: "Legitimate",      cls: "bg-green-900 text-green-300 border border-green-700" },
  UNKNOWN:         { label: "Unknown",         cls: "bg-gray-800 text-gray-400 border border-gray-600" },
  UNANALYZED:      { label: "Pending Analysis",cls: "bg-slate-800 text-slate-400 border border-slate-600" },
};

export function ClassificationBadge({
  classification,
}: {
  classification: Classification | null | undefined;
}) {
  const key = (classification ?? "UNANALYZED") as keyof typeof classConfig;
  const { label, cls } = classConfig[key] ?? classConfig.UNANALYZED;
  return (
    <span className={clsx("text-xs font-medium px-2 py-0.5 rounded-full whitespace-nowrap", cls)}>
      {label}
    </span>
  );
}

export function ScoreBadge({ score }: { score: number | null | undefined }) {
  if (score == null) {
    return (
      <span className="text-xs text-slate-500">—</span>
    );
  }
  const cls =
    score >= 80
      ? "text-red-400"
      : score >= 60
      ? "text-orange-400"
      : score >= 40
      ? "text-yellow-400"
      : "text-green-400";

  return (
    <span className={clsx("font-mono font-semibold text-sm", cls)}>
      {score}
    </span>
  );
}

export function AnomalyBadge({ code }: { code: string }) {
  const colors: Record<string, string> = {
    VELOCITY:     "bg-purple-900 text-purple-300 border border-purple-700",
    AMOUNT_SPIKE: "bg-blue-900 text-blue-300 border border-blue-700",
    GEO_TRAVEL:   "bg-teal-900 text-teal-300 border border-teal-700",
  };
  const cls = colors[code] ?? "bg-gray-800 text-gray-400 border border-gray-600";
  return (
    <span className={clsx("text-xs font-mono px-2 py-0.5 rounded-full", cls)}>
      {code}
    </span>
  );
}
