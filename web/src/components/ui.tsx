import type { ReactNode } from "react";
import type { RunStatus, TaskStatus } from "../api/client";

type Status = RunStatus | TaskStatus;

// Task "canceled" reuses the neutral pending palette.
const palette: Record<Status, string> = {
  pending: "pending",
  claimed: "claimed",
  sleeping: "sleeping",
  completed: "completed",
  failed: "failed",
  canceled: "pending",
};

export function StatusPill({ status, label }: { status: Status; label?: string }) {
  const key = palette[status];
  return (
    <span className="pill" style={{ background: `var(--st-${key}-bg)`, color: `var(--st-${key}-fg)` }}>
      <span className="dot" />
      {label ?? status}
    </span>
  );
}

export function StatTile({ label, value, sub, tone }: { label: string; value: ReactNode; sub?: ReactNode; tone?: "warn" | "danger" }) {
  return (
    <div className="card tile">
      <div className="tile-label">{label}</div>
      <div className="tile-value" style={tone ? { color: `var(--${tone})` } : undefined}>
        {value}
      </div>
      {sub !== undefined && <div className="tile-sub">{sub}</div>}
    </div>
  );
}

export function Card({ title, aside, children }: { title?: ReactNode; aside?: ReactNode; children: ReactNode }) {
  return (
    <section className="card">
      {(title || aside) && (
        <div className="card-head">
          {title && <h2>{title}</h2>}
          {aside && <span className="muted" style={{ fontSize: 12 }}>{aside}</span>}
        </div>
      )}
      {children}
    </section>
  );
}

export function Loading({ what = "Loading" }: { what?: string }) {
  return <div className="state">{what}…</div>;
}

export function ErrorState({ error }: { error: unknown }) {
  const message = error instanceof Error ? error.message : String(error);
  const unreachable = error instanceof TypeError || /^50\d/.test(message);
  return (
    <div className="state error" role="alert">
      {message}
      {unreachable && <div className="muted" style={{ marginTop: 6 }}>Is the API server running on localhost:8080?</div>}
    </div>
  );
}

export function Empty({ children }: { children: ReactNode }) {
  return <div className="state">{children}</div>;
}

// Marks a part of the UI that waits for an endpoint the API doesn't have yet.
export function NeedsEndpoint({ title, endpoint, children }: { title: string; endpoint: string; children?: ReactNode }) {
  return (
    <div className="placeholder">
      <strong style={{ color: "var(--ink)" }}>{title}</strong> needs <code>{endpoint}</code>.
      {children && <div style={{ marginTop: 6 }}>{children}</div>}
    </div>
  );
}
