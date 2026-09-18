"use client";

import React, { useState, useEffect } from "react";
import { ParticipantList, Participant } from "@/components/ParticipantList";
import { BoardState, ClaimItem, FindingItem } from "@/components/BoardState";
import { ConflictViewer, ConflictItem } from "@/components/ConflictViewer";
import { VoiceControl } from "@/components/VoiceControl";
import { Play, RotateCcw, Activity, Shield, Terminal } from "lucide-react";

export default function WorkspacePage() {
  const [question, setQuestion] = useState(
    "What are the core technical trade-offs between centralized and decentralized multi-agent coordination architectures?"
  );
  const [isRunning, setIsRunning] = useState(false);

  const [participants, setParticipants] = useState<Participant[]>([
    { id: "human-user", name: "Human Operator", type: "human", role: "Operator", status: "active" },
    { id: "agent-1-literature", name: "Literature Researcher", type: "agent", role: "Prior Art", status: "idle" },
    { id: "agent-2-data", name: "Data Analyst", type: "agent", role: "Quantitative", status: "idle" },
    { id: "agent-3-critique", name: "Critique Specialist", type: "agent", role: "Adversarial", status: "idle" },
    { id: "agent-4-synthesis", name: "Synthesis Builder", type: "agent", role: "Consensus", status: "idle" },
    { id: "adjudicator", name: "Adjudicator", type: "adjudicator", role: "Conflict Detector", status: "active" },
    { id: "reaper", name: "Reaper (TTL=15s)", type: "reaper", role: "Liveness Garbage Collector", status: "active" },
  ]);

  const [claims, setClaims] = useState<ClaimItem[]>([
    {
      id: "claim-1",
      participantId: "agent-1-literature",
      participantType: "agent",
      content: "Prior art analysis on decentralized consensus in multi-agent swarms",
      status: "active",
      lastHeartbeat: new Date().toISOString(),
      createdAt: new Date().toISOString(),
    },
    {
      id: "claim-2",
      participantId: "agent-2-data",
      participantType: "agent",
      content: "Quantitative latency benchmarks for in-process memory buses",
      status: "active",
      lastHeartbeat: new Date().toISOString(),
      createdAt: new Date().toISOString(),
    },
  ]);

  const [findings, setFindings] = useState<FindingItem[]>([
    {
      id: "finding-1",
      participantId: "agent-1-literature",
      participantType: "agent",
      title: "Decentralized Coordination Trade-offs",
      content: "Decentralized topologies offer high fault tolerance and eliminate single-point bottlenecks but suffer from higher communication complexity O(N^2) without a shared semantic board.",
      sources: ["arxiv.2308.08155", "moss-whitepaper-2024"],
      createdAt: new Date().toISOString(),
    },
    {
      id: "finding-2",
      participantId: "agent-2-data",
      participantType: "agent",
      title: "In-Process Memory Bus Latency Advantage",
      content: "Eliminating the HTTP retrieval hop reduces semantic sense() latency from ~150ms to sub-1ms (0.82ms p50), making check-before-act viable on every single step.",
      sources: ["quorum-day1-benchmark"],
      createdAt: new Date().toISOString(),
    },
  ]);

  const [conflicts, setConflicts] = useState<ConflictItem[]>([
    {
      id: "conf-1",
      findingAId: "finding-1",
      findingBId: "finding-2",
      similarityScore: 0.88,
      adjudicatorVerdict: "Finding 1 emphasizes O(N^2) communication complexity in pure message-passing, whereas Finding 2 demonstrates that shared in-process boards reduce coordination overhead to O(1) per agent check.",
      resolved: false,
      createdAt: new Date().toISOString(),
    },
  ]);

  const handleResolveConflict = (id: string) => {
    setConflicts((prev) =>
      prev.map((c) => (c.id === id ? { ...c, resolved: true } : c))
    );
  };

  const handleRun = () => {
    setIsRunning(true);
    // Cycle agent statuses to demonstrate active coordination
    setParticipants((prev) =>
      prev.map((p) =>
        p.type === "agent" ? { ...p, status: "sensing" } : p
      )
    );

    setTimeout(() => {
      setParticipants((prev) =>
        prev.map((p) =>
          p.type === "agent" ? { ...p, status: "acting" } : p
        )
      );
    }, 1200);

    setTimeout(() => {
      setParticipants((prev) =>
        prev.map((p) =>
          p.type === "agent" ? { ...p, status: "active" } : p
        )
      );
      setIsRunning(false);
    }, 2500);
  };

  return (
    <main className="min-h-screen p-6 max-w-7xl mx-auto flex flex-col gap-6">
      {/* Top Banner */}
      <header className="flex items-center justify-between p-4 rounded-2xl bg-[#111827]/80 backdrop-blur border border-slate-800">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-xl bg-gradient-to-tr from-cyan-500 to-emerald-500 flex items-center justify-center font-bold text-slate-900 text-lg shadow-lg shadow-cyan-500/20">
            Q
          </div>
          <div>
            <h1 className="text-lg font-bold text-slate-100 flex items-center gap-2">
              QUORUM <span className="text-xs font-mono text-cyan-400 bg-cyan-500/10 px-2 py-0.5 rounded border border-cyan-500/20">v0.1.0</span>
            </h1>
            <p className="text-xs text-slate-400">
              Single-Process Multi-Agent Coordination • In-Process Moss Semantic Board • LiveKit Transport
            </p>
          </div>
        </div>

        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-slate-800/80 border border-slate-700 text-xs font-mono text-slate-300">
            <Activity className="w-4 h-4 text-cyan-400" />
            <span>Process: In-Memory (Zero HTTP Hop)</span>
          </div>
        </div>
      </header>

      {/* Query Bar */}
      <div className="flex gap-3 p-3 rounded-2xl bg-[#111827]/80 backdrop-blur border border-slate-800 items-center">
        <Terminal className="w-5 h-5 text-cyan-400 ml-2" />
        <input
          type="text"
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          className="flex-1 bg-transparent border-none outline-none text-sm text-slate-200 placeholder-slate-500 font-mono"
          placeholder="Enter research inquiry for coordinating agent swarm..."
        />
        <button
          onClick={handleRun}
          disabled={isRunning}
          className="flex items-center gap-2 px-5 py-2.5 rounded-xl bg-gradient-to-r from-cyan-500 to-emerald-500 hover:from-cyan-400 hover:to-emerald-400 text-slate-950 font-semibold text-xs transition shadow-lg shadow-cyan-500/20 disabled:opacity-50"
        >
          <Play className="w-4 h-4 fill-current" />
          {isRunning ? "Coordinating..." : "Execute Swarm"}
        </button>
      </div>

      {/* Main Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 flex-1">
        {/* Left Column: Swarm Presence & Voice */}
        <div className="flex flex-col gap-6">
          <ParticipantList participants={participants} />
          <VoiceControl />
          <ConflictViewer conflicts={conflicts} onResolve={handleResolveConflict} />
        </div>

        {/* Right 2 Columns: Shared Semantic Board */}
        <div className="lg:col-span-2 flex flex-col gap-6">
          <BoardState claims={claims} findings={findings} senseLatencyP50={0.82} />
        </div>
      </div>
    </main>
  );
}
