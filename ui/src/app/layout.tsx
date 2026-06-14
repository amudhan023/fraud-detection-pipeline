import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Fraud Detection — AI Dashboard",
  description: "Real-time fraud event analysis powered by Claude AI",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>
        <header
          className="sticky top-0 z-30 border-b"
          style={{ background: "var(--surface)", borderColor: "var(--border)" }}
        >
          <div className="mx-auto max-w-screen-2xl px-6 h-14 flex items-center gap-3">
            <div className="w-7 h-7 rounded-md bg-red-500 flex items-center justify-center text-white font-bold text-sm">
              F
            </div>
            <span className="font-semibold text-white tracking-tight">
              Fraud Detection
            </span>
            <span
              className="text-xs px-2 py-0.5 rounded-full ml-1"
              style={{ background: "var(--surface-2)", color: "var(--text-muted)" }}
            >
              AI-powered
            </span>
          </div>
        </header>
        <main className="mx-auto max-w-screen-2xl px-6 py-6">{children}</main>
      </body>
    </html>
  );
}
