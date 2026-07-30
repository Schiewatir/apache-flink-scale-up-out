/**
 * Shortens a Flink job-vertex name into a stable, human-readable series key for
 * the telemetry timeline. Operator names come from the job jar and are stable
 * across jobIds (redeploys), but chained vertex names are long
 * (e.g. "Source: kafka-source -> parse-json -> drop-invalid"): use the first
 * chained operator, without the "Source:" prefix.
 */
export function shortVertexName(name: string): string {
  const first = name
    .replace(/^Source:\s*/i, '')
    .split('->')[0]
    ?.trim();
  return first && first.length > 0 ? first : name.trim();
}
