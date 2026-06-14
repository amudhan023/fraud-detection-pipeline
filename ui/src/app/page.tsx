"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import type { DashboardStats, FraudEvent } from "@/lib/api";
import { api } from "@/lib/api";
import { StatsCards } from "@/components/StatsCards";
import { FraudEventsTable } from "@/components/FraudEventsTable";
import { EventDetailPanel } from "@/components/EventDetailPanel";
import { ClassificationBadge } from "@/components/RiskBadge";
import type { Classification } from "@/lib/api";

const REFRESH_INTERVAL = 10_000;

const CLASSIFICATIONS: (Classification | "ALL")[] = [
  "ALL",
  "CONFIRMED_FRAUD",
  "LIKELY_FRAUD",
  "SUSPICIOUS",
  "LEGITIMATE",
  "UNKNOWN",
];

export default function DashboardPage() {
  const [stats, setStats] = useState<DashboardStats | null>(null);
  const [events, setEvents] = useState<FraudEvent[]>([]);
  const [selected, setSelected] = useState<FraudEvent | null>(null);
  const [filter, setFilter] = useState<Classification | "ALL">("ALL");
  const [search, setSearch] = useState("");
  const [loading, setLoading] = useState(true);
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const refresh = useCallback(async () => {
    try {
      const [s, ev] = await Promise.all([api.stats(), api.events(200)]);
      setStats(s);
      setEvents(ev);
      setLastRefresh(new Date());
      if (selected) {
        const updated = ev.find((e) => e.id === selected.id);
        if (updated) setSelected(updated);
      }
    } catch (err) {
      console.error("Refresh failed:", err);
    } finally {
      setLoading(false);
    }
  }, [selected]);

  useEffect(() => {
    refresh();
    intervalRef.current = setInterval(refresh, REFRESH_INTERVAL);
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [refresh]);

  const afterAction = () => {
    refresh();
    if (selected) {
      api.event(selected.id).then(setSelected).catch(console.error);
    }
  };

  const filtered = events.filter((ev) => {
    const matchClass =
      filter === "ALL" || ev.ai_analysis?.classification === filter;
    const q = search.toLowerCase();
    const matchSearch =
      !q ||
      ev.transaction_id.toLowerCase().includes(q) ||
      (ev.owner_name ?? "").toLowerCase().includes(q) ||
      (ev.merchant_name ?? "").toLowerCase().includes(q) ||
      ev.reason_code.toLowerCase().includes(q);
    return matchClass && matchSearch;
  });

  return (
    <div className="space-y-6">
      {/* Page header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Fraud Events</h1>
          <p className="text-sm mt-0.5" style={{ color: "var(--text-muted)" }}>
            AI-powered anomaly classification · auto-refresh every 10s
          </p>
        </div>
        <div className="flex items-center gap-3">
          {lastRefresh && (
            <span className="text-xs" style={{ color: "var(--text-muted)" }}>
              Updated {lastRefresh.toLocaleTimeString()}
            </span>
          )}
          <button
            onClick={() => { setLoading(true); refresh(); }}
            className="text-xs px-3 py-1.5 rounded-md border transition-colors hover:bg-white/5"
            style={{ borderColor: "var(--border)", color: "var(--text-muted)" }}
          >
            Refresh
          </button>
        </div>
      </div>

      {/* Stats */}
      {stats ? (
        <StatsCards stats={stats} />
      ) : (
        <div className="grid grid-cols-5 gap-4 mb-6">
          {[...Array(5)].map((_, i) => (
            <div
              key={i}
              className="rounded-xl border h-24 animate-pulse"
              style={{ background: "var(--surface)", borderColor: "var(--border)" }}
            />
          ))}
        </div>
      )}

      {/* Filters */}
      <div className="flex flex-wrap gap-3 items-center">
        <input
          type="search"
          placeholder="Search by transaction ID, account, merchant…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="text-sm rounded-lg px-3 py-2 border outline-none w-72"
          style={{
            background: "var(--surface)",
            borderColor: "var(--border)",
            color: "var(--text)",
          }}
        />
        <div className="flex gap-2 flex-wrap">
          {CLASSIFICATIONS.map((c) => (
            <button
              key={c}
              onClick={() => setFilter(c)}
              className={`text-xs px-3 py-1 rounded-full border transition-colors ${
                filter === c
                  ? "border-blue-500 bg-blue-900/30 text-blue-300"
                  : "border-transparent hover:bg-white/5 text-slate-400"
              }`}
            >
              {c === "ALL" ? (
                "All"
              ) : (
                <ClassificationBadge classification={c as Classification} />
              )}
            </button>
          ))}
        </div>
        <span className="text-xs ml-auto" style={{ color: "var(--text-muted)" }}>
          {filtered.length} event{filtered.length !== 1 ? "s" : ""}
        </span>
      </div>

      {/* Table */}
      {loading ? (
        <div
          className="rounded-xl border h-64 flex items-center justify-center"
          style={{ background: "var(--surface)", borderColor: "var(--border)" }}
        >
          <p style={{ color: "var(--text-muted)" }}>Loading events…</p>
        </div>
      ) : (
        <FraudEventsTable
          events={filtered}
          selectedId={selected?.id ?? null}
          onSelect={setSelected}
        />
      )}

      {/* Detail panel (overlay) */}
      {selected && (
        <>
          <div
            className="fixed inset-0 z-30 bg-black/40 backdrop-blur-sm"
            onClick={() => setSelected(null)}
          />
          <EventDetailPanel
            event={selected}
            onClose={() => setSelected(null)}
            onActionRecorded={afterAction}
          />
        </>
      )}
    </div>
  );
}
