"use client";

import React, { useState } from "react";
import { BookOpen, CheckCircle, Clock, Zap, Layers } from "lucide-react";

export interface ClaimItem {
  id: string;
  participantId: string;
  participantType: string;
  content: string;
  status: "active" | "superseded" | "resolved" | "reaped";
  supersedes?: string | null;
  lastHeartbeat: string;
  createdAt: string;
}

export interface FindingItem {
  id: string;
  participantId: string;
  participantType: string;
  title: string;
  content: string;
  sources: string[];
  createdAt: string;
}

interface BoardStateProps {
  claims: ClaimItem[];
  findings: FindingItem[];
  senseLatencyP50?: number;
}

export const BoardState: React.FC<BoardStateProps> = ({
  claims,
  findings,
  senseLatencyP50 = 0.82,
}) => {
  const [tab, setTab] = useState<"findings" | "claims">("findings");

  return (
    <div className="bg-[#111827]/80 backdrop-blur border border-slate-800 rounded-xl p-5 flex flex-col gap-4 flex-1">
      {/* Header with latency indicator */}
      <div className="flex items-center justify-between pb-3 border-b border-slate-800">
        <div className="flex items-center gap-3">
          <div className="p-2 rounded-lg bg-cyan-500/10 border border-cyan-500/20 text-cyan-400">
            <Layers className="w-5 h-5" />
          </div>
          <div>
            <h2 className="text-base font-semibold text-slate-100">
              In-Process Semantic Board (Moss)
            </h2>
            <p className="text-xs text-slate-400">
              Zero network hop • Direct memory space indexing
            </p>
          </div>
        </div>

        <div className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-emerald-500/10 border border-emerald-500/20">
          <Zap className="w-4 h-4 text-emerald-400" />
          <span className="text-xs font-mono text-emerald-400 font-semibold">
            sense() p50: {senseLatencyP50}ms
          </span>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex gap-2">
        <button
          onClick={() => setTab("findings")}
          className={`px-4 py-2 text-xs font-semibold rounded-lg transition ${
            tab === "findings"
              ? "bg-cyan-500/20 text-cyan-300 border border-cyan-500/40"
              : "bg-slate-800/40 text-slate-400 hover:text-slate-200 border border-transparent"
          }`}
        >
          Synthesized Findings ({findings.length})
        </button>
        <button
          onClick={() => setTab("claims")}
          className={`px-4 py-2 text-xs font-semibold rounded-lg transition ${
            tab === "claims"
              ? "bg-cyan-500/20 text-cyan-300 border border-cyan-500/40"
              : "bg-slate-800/40 text-slate-400 hover:text-slate-200 border border-transparent"
          }`}
        >
          Active Subtask Claims ({claims.length})
        </button>
      </div>

      {/* Content Stream */}
      <div className="flex flex-col gap-3 overflow-y-auto max-h-[480px] pr-1">
        {tab === "findings" ? (
          findings.length === 0 ? (
            <div className="py-12 text-center text-slate-500 text-sm">
              No findings published to board yet. Agents are actively sensing...
            </div>
          ) : (
            findings.map((f) => (
              <div
                key={f.id}
                className="p-4 rounded-xl bg-[#162032]/80 border border-slate-800 hover:border-slate-700 transition flex flex-col gap-2"
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <BookOpen className="w-4 h-4 text-cyan-400" />
                    <span className="text-sm font-semibold text-slate-100">{f.title}</span>
                  </div>
                  <span className="text-xs text-slate-400 font-mono">
                    by {f.participantId}
                  </span>
                </div>
                <p className="text-xs text-slate-300 leading-relaxed">{f.content}</p>
                {f.sources && f.sources.length > 0 && (
                  <div className="flex flex-wrap gap-1.5 pt-1">
                    {f.sources.map((src, i) => (
                      <span
                        key={i}
                        className="px-2 py-0.5 text-[10px] font-mono rounded bg-slate-800 text-slate-400 border border-slate-700"
                      >
                        {src}
                      </span>
                    ))}
                  </div>
                )}
              </div>
            ))
          )
        ) : claims.length === 0 ? (
          <div className="py-12 text-center text-slate-500 text-sm">
            No active claims on the board.
          </div>
        ) : (
          claims.map((c) => (
            <div
              key={c.id}
              className="p-3.5 rounded-xl bg-[#162032]/80 border border-slate-800 flex items-center justify-between"
            >
              <div className="flex flex-col gap-1">
                <span className="text-xs font-mono text-slate-200">{c.content}</span>
                <span className="text-[10px] text-slate-400">
                  Claimed by {c.participantId} ({c.participantType})
                  {c.supersedes && ` • supersedes ${c.supersedes.slice(0, 8)}`}
                </span>
              </div>
              <span
                className={`px-2 py-0.5 text-[10px] font-mono rounded border ${
                  c.status === "active"
                    ? "bg-emerald-500/10 text-emerald-400 border-emerald-500/20"
                    : c.status === "reaped"
                    ? "bg-rose-500/10 text-rose-400 border-rose-500/20"
                    : "bg-slate-700/30 text-slate-400 border-slate-700"
                }`}
              >
                {c.status.toUpperCase()}
              </span>
            </div>
          ))
        )}
      </div>
    </div>
  );
};
