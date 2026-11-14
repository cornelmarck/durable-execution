import { NavLink, Outlet } from "react-router";
import { useQueues } from "../api/hooks";

const links = [
  { to: "/", label: "Overview", end: true },
  { to: "/queues", label: "Queues" },
  { to: "/tasks", label: "Tasks" },
];

// Screens from the sketch that wait for read endpoints the API doesn't have yet.
const comingSoon = ["Workflow runs", "Events"];

export function Layout() {
  return (
    <div className="shell">
      <nav aria-label="Main" className="sidebar">
        <div className="brand">
          durable<span>·</span>console
        </div>
        {links.map((l) => (
          <NavLink key={l.to} to={l.to} end={l.end} className="nav-link">
            {l.label}
          </NavLink>
        ))}
        {comingSoon.map((label) => (
          <span key={label} className="nav-link disabled" aria-disabled="true" title="Needs a list endpoint in the API">
            {label}
            <span className="nav-tag">soon</span>
          </span>
        ))}
        <Health />
      </nav>
      <main className="content">
        <Outlet />
      </main>
    </div>
  );
}

function Health() {
  const { isError, isPending, dataUpdatedAt } = useQueues();
  const color = isError ? "var(--danger)" : isPending ? "#9da1ab" : "#3fb46a";
  const label = isError ? "API unreachable" : isPending ? "connecting" : "live · 5s refresh";
  return (
    <div className="health" title={dataUpdatedAt ? `Last update ${new Date(dataUpdatedAt).toLocaleTimeString()}` : undefined}>
      <span className="dot" style={{ background: color }} />
      {label}
    </div>
  );
}
