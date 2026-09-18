"use client";

import React from "react";
import { AlertTriangle, Check, ShieldCheck } from "lucide-react";

export interface ConflictItem {
  id: string;
  findingAId: string;
  findingBId: string;
  similarityScore: number;
  adjudicatorVerdict: string | null;
  resolved: boolean;
  createdAt: string;
}

interface ConflictViewerProps {
  conflicts: ConflictItem[];
  onResolve: (id: string) => void;
}

export const ConflictViewer: React.FC<ConflictViewerProps> = ({
  conflicts,
  onResolve,
}) => {
  const unresolved = conflicts.filter((c) => !c.resolved);

  return (
    <div className="bg-[#111827]/80 backdrop-blur border border-slate-800 rounded-xl p-4 flex flex-col gap-3">
      <div className="flex items-center justify-between pb-2 border-b border-slate-800">
        <div className="flex items-center gap-2">
          <AlertTriangle className="w-4 h-4 text-amber-400" />
          <h3 className="text-sm font-semibold tracking-wider text-slate-300 uppercase">
            Adjudication Tray ({unresolved.length} pending)
          </h3>
        </div>
        <span className="text-[10px] text-slate-400 font-mono">
          Human Resolution Gated
        </span>
      </div>

      <div className="flex flex-col gap-2.5 overflow-y-auto max-h-[250px]">
        {unresolved.length === 0 ? (
          <div className="py-6 text-center text-slate-500 text-xs flex flex-col items-center gap-1.5">
            <ShieldCheck className="w-5 h-5 text-emerald-500/50" />
            <span>No unresolved conflicts flagged by Adjudicator.</span>
          </div>
        ) : (
          unresolved.map((c) => (
            <div
              key={c.id}
              className="p-3 rounded-lg bg-amber-500/5 border border-amber-500/20 flex flex-col gap-2"
            >
              <div className="flex items-center justify-between">
                <span className="text-xs font-mono text-amber-300 font-medium">
                  Semantic Similarity: {(c.similarityScore * 100).toFixed(1)}%
                </span>
                <button
                  onClick={() => onResolve(c.id)}
                  className="flex items-center gap-1 px-2.5 py-1 rounded bg-emerald-500/20 hover:bg-emerald-500/30 text-emerald-300 border border-emerald-500/30 text-xs font-medium transition"
                >
                  <Check className="w-3 h-3" /> Resolve
                </button>
              </div>
              <p className="text-xs text-slate-300 italic bg-black/30 p-2 rounded border border-slate-800/60">
                "{c.adjudicatorVerdict || "Adjudicator analyzing conflicting findings..."}"
              </p>
            </div>
          ))
        )}
      </div>
    </div>
  );
};
