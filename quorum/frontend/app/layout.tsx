import type { Metadata } from "next";
import "./globals.css";
import "@livekit/components-styles";

export const metadata: Metadata = {
  title: "Quorum — Real-Time Multi-Agent Coordination Workspace",
  description: "Single-process multi-agent semantic board with in-process Moss indexing and LiveKit transport",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark">
      <body className="bg-[#090d16] text-[#f0f4f8] antialiased min-h-screen">
        {children}
      </body>
    </html>
  );
}
