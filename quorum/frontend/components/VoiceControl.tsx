"use client";

import React, { useState } from "react";
import { Mic, MicOff, Volume2, Radio } from "lucide-react";

interface VoiceControlProps {
  onSendVoiceQuery?: (transcript: string) => void;
}

export const VoiceControl: React.FC<VoiceControlProps> = ({ onSendVoiceQuery }) => {
  const [isRecording, setIsRecording] = useState(false);
  const [transcript, setTranscript] = useState<string | null>(null);

  const toggleRecording = () => {
    if (isRecording) {
      setIsRecording(false);
      // Simulated transcription with immediate audio zeroing
      const sampleQuery = "What are the latest findings on decentralized consensus?";
      setTranscript(sampleQuery);
      if (onSendVoiceQuery) {
        onSendVoiceQuery(sampleQuery);
      }
    } else {
      setIsRecording(true);
      setTranscript(null);
    }
  };

  return (
    <div className="bg-[#111827]/80 backdrop-blur border border-slate-800 rounded-xl p-4 flex flex-col gap-3">
      <div className="flex items-center justify-between pb-2 border-b border-slate-800">
        <div className="flex items-center gap-2">
          <Radio className="w-4 h-4 text-purple-400" />
          <h3 className="text-sm font-semibold tracking-wider text-slate-300 uppercase">
            LiveKit Voice Bridge
          </h3>
        </div>
        <span className="text-[10px] font-mono text-purple-300 bg-purple-500/10 px-2 py-0.5 rounded border border-purple-500/20">
          50ms Human Priority Epsilon
        </span>
      </div>

      <div className="flex items-center gap-3">
        <button
          onClick={toggleRecording}
          className={`flex items-center justify-center p-3 rounded-xl border transition ${
            isRecording
              ? "bg-rose-500/20 border-rose-500/40 text-rose-300 animate-pulse"
              : "bg-slate-800/60 border-slate-700/60 text-slate-300 hover:bg-slate-800"
          }`}
        >
          {isRecording ? <Mic className="w-5 h-5" /> : <MicOff className="w-5 h-5" />}
        </button>

        <div className="flex flex-col flex-1">
          <span className="text-xs font-medium text-slate-200">
            {isRecording ? "Listening (audio will be discarded after transcription)..." : "Push to speak voice query / claim"}
          </span>
          {transcript && (
            <div className="flex items-center gap-1.5 mt-1 text-xs text-purple-300">
              <Volume2 className="w-3.5 h-3.5" />
              <span>"{transcript}"</span>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};
