export function formatDuration(seconds: number): string {
  // API timestamps have one-second precision, so don't pretend to more.
  if (seconds < 1) return "<1s";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  const units: [string, number][] = [
    ["d", 86400],
    ["h", 3600],
    ["m", 60],
    ["s", 1],
  ];
  const parts: string[] = [];
  let rest = Math.floor(seconds);
  for (const [label, size] of units) {
    const n = Math.floor(rest / size);
    if (n > 0) parts.push(`${n}${label}`);
    rest -= n * size;
    if (parts.length === 2) break;
  }
  return parts.join(" ");
}

export function timeAgo(iso: string, now = Date.now()): string {
  const seconds = (now - new Date(iso).getTime()) / 1000;
  if (seconds < 5) return "just now";
  return `${formatDuration(seconds)} ago`;
}

export function between(startIso: string, endIso?: string): string | undefined {
  if (!endIso) return undefined;
  return formatDuration((new Date(endIso).getTime() - new Date(startIso).getTime()) / 1000);
}

export function formatDateTime(iso: string): string {
  return new Date(iso).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "medium" });
}

export function formatNumber(n: number): string {
  return n.toLocaleString();
}

// UUIDv7 ids share a time-based prefix, so show the start and the end.
export function shortId(id: string): string {
  return id.length > 16 ? `${id.slice(0, 8)}…${id.slice(-4)}` : id;
}

export function formatTtl(seconds?: number): string {
  if (seconds === undefined) return "default";
  if (seconds === 0) return "never";
  return formatDuration(seconds);
}
