import { spawn } from 'node:child_process';

export interface ExecResult {
  code: number;
  stdout: string;
  stderr: string;
  timedOut: boolean;
}

export interface ExecOptions {
  cwd?: string;
  timeoutMs?: number;
  env?: NodeJS.ProcessEnv;
}

/**
 * Run a command with a fixed argument vector (never a shell string) and capture
 * its output. Using an argument vector — not a shell — means client-derived
 * values can never be interpreted as shell syntax.
 */
export function execCapture(
  command: string,
  args: string[],
  options: ExecOptions = {},
): Promise<ExecResult> {
  const { cwd, timeoutMs = 20_000, env } = options;
  return new Promise((resolve) => {
    const child = spawn(command, args, { cwd, env, shell: false });
    let stdout = '';
    let stderr = '';
    let timedOut = false;

    const timer = setTimeout(() => {
      timedOut = true;
      child.kill('SIGKILL');
    }, timeoutMs);

    child.stdout.on('data', (chunk: Buffer) => {
      stdout += chunk.toString('utf8');
    });
    child.stderr.on('data', (chunk: Buffer) => {
      stderr += chunk.toString('utf8');
    });
    child.on('error', (err) => {
      clearTimeout(timer);
      resolve({ code: -1, stdout, stderr: stderr + String(err), timedOut });
    });
    child.on('close', (code) => {
      clearTimeout(timer);
      resolve({ code: code ?? -1, stdout, stderr, timedOut });
    });
  });
}

/** Convenience wrapper for `kubectl` invocations. */
export function kubectl(args: string[], options: ExecOptions = {}): Promise<ExecResult> {
  return execCapture('kubectl', args, options);
}
