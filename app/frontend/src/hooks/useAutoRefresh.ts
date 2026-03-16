import { useState, useEffect } from "react";

export function useAutoRefresh(intervalMs = 15000) {
  const [key, setKey] = useState(0);

  useEffect(() => {
    const id = setInterval(() => setKey((k) => k + 1), intervalMs);
    return () => clearInterval(id);
  }, [intervalMs]);

  return key;
}
