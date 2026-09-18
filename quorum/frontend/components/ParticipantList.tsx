"use client";

import React from "react";
import { User, Bot, ShieldAlert, Cpu } from "lucide-react";

export interface Participant {
  id: string;
  name: string;
  type: "human" | "agent" | "adjudicator" | "reaper";
  role?: string;
  status: "active" | "idle" | "sensing" | "claiming" | "acting";
  lastHeartbeat?: string;
}

interface ParticipantListProps {
  participants: Participant[];
}

export const ParticipantList: React.FC<ParticipantListProps> = ({ participants }) => {
  const getIcon = (type: Participant["type"]) => {
    switch (type) {
      case "human":
        return <User className="w-4 h-4 text-emerald-400" />;
      case "adjudicator":
        return <ShieldAlert className="w-4 h-4 text-amber-400" />;
      case "reaper":
        return <Cpu className="w-4 h-4 text-rose-400" />;
      default:
        return <Bot className="w-4 h-4 text-cyan-400" />;
    }
  };

  const getStatusBadge = (status: Participant["status"]) => {
    const map = {
      active: "bg-emerald-500/10 text-emerald-400 border-emerald-500/20",
      idle: "bg-slate-500/10 text-slate-400 border-slate-500/20",
      sensing: "bg-cyan-500/10 text-cyan-400 border-cyan-500/20 animate-pulse",
      claiming: "bg-purple-500/10 text-purple-400 border-purple-500/20 animate-pulse",
      acting: "bg-amber-500/10 text-amber-400 border-amber-500/20",
    };
    return (
      <span className={`px-2 py-0.5 text-xs font-mono rounded border ${map[status] || map.idle}`}>
        {status.toUpperCase()}
      </span>
    );
  };

  return (
    <div className="bg-[#111827]/80 backdrop-blur border border-slate-800 rounded-xl p-4 flex flex-col gap-3">
      <div className="flex items-center justify-between pb-2 border-b border-slate-800">
        <h3 className="text-sm font-semibold tracking-wider text-slate-300 uppercase">
          Coordinating Swarm ({participants.length})
        </h3>
        <span className="flex h-2 w-2 relative">
          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
          <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
        </span>
      </div>

      <div className="flex flex-col gap-2 overflow-y-auto max-h-[300px]">
        {participants.map((p) => (
          <div
            key={p.id}
            className="flex items-center justify-between p-2.5 rounded-lg bg-[#162032]/60 hover:bg-[#162032] border border-slate-800/80 transition"
          >
            <div className="flex items-center gap-2.5">
              <div className="p-1.5 rounded-md bg-slate-800/60 border border-slate-700/50">
                {getIcon(p.type)}
              </div>
              <div className="flex flex-col">
                <span className="text-sm font-medium text-slate-200">{p.name}</span>
                <span className="text-xs text-slate-400 font-mono">{p.role || p.type}</span>
              </div>
            </div>
            {getStatusBadge(p.status)}
          </div>
        ))}
      </div>
    </div>
  );
};
