import type {
  PrometheusRuntimeTelemetrySink,
  RuntimeTelemetryEvent,
  RuntimeTelemetryFlusher,
  RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

export interface SchedulerOperationalMetricControls {
  setExpectedActive?(expected: boolean): void;
  setLastSuccessTimestamp?(timestampSeconds: number): void;
}

export type SchedulerMetricsSink = Omit<PrometheusRuntimeTelemetrySink, "recordDependencyLatency"> & SchedulerOperationalMetricControls;

export function bestEffortTelemetrySink(
  sink: RuntimeTelemetrySink | undefined
): RuntimeTelemetrySink | undefined {
  if (sink === undefined) {
    return undefined;
  }

  return {
    emit: async (event) => {
      try {
        await sink.emit(event);
      } catch {
        // Observability is never part of the scheduler's delivery state machine.
      }
    }
  };
}

export function combineBestEffortTelemetrySinks(
  ...sinks: readonly (RuntimeTelemetrySink | undefined)[]
): RuntimeTelemetrySink | undefined {
  const configured = sinks
    .map((sink) => bestEffortTelemetrySink(sink))
    .filter((sink): sink is RuntimeTelemetrySink => sink !== undefined);

  if (configured.length === 0) {
    return undefined;
  }

  return {
    emit: async (event) => {
      for (const sink of configured) {
        await sink.emit(event);
      }
    }
  };
}

export function bestEffortTelemetryFlusher(
  sink: (RuntimeTelemetrySink & RuntimeTelemetryFlusher) | undefined
): (RuntimeTelemetrySink & RuntimeTelemetryFlusher) | undefined {
  if (sink === undefined) {
    return undefined;
  }

  return {
    emit: async (event) => {
      try {
        await sink.emit(event);
      } catch {
        // A failed log write cannot alter scheduler lifecycle behavior.
      }
    },
    flush: async () => {
      try {
        await sink.flush();
      } catch {
        // A failed log flush cannot block graceful shutdown.
      }
    }
  };
}

export function bestEffortSchedulerMetricsSink(
  sink: SchedulerMetricsSink | undefined
): SchedulerMetricsSink | undefined {
  if (sink === undefined) {
    return undefined;
  }

  const controls: SchedulerOperationalMetricControls = {
    ...(typeof sink.setExpectedActive === "function" ? {
      setExpectedActive: (expected: boolean): void => {
        safely(() => sink.setExpectedActive?.(expected));
      }
    } : {}),
    ...(typeof sink.setLastSuccessTimestamp === "function" ? {
      setLastSuccessTimestamp: (timestampSeconds: number): void => {
        safely(() => sink.setLastSuccessTimestamp?.(timestampSeconds));
      }
    } : {})
  };

  return {
    allowedLabels: sink.allowedLabels,
    emit: async (event: RuntimeTelemetryEvent): Promise<void> => {
      try {
        await sink.emit(event);
      } catch {
        // Metrics rejection cannot alter scheduling, lease, or publish state.
      }
    },
    collect: (): string => {
      try {
        return sink.collect();
      } catch {
        return "";
      }
    },
    setInFlight: (queue, value): void => {
      safely(() => sink.setInFlight(queue, value));
    },
    setShutdownDraining: (draining): void => {
      safely(() => sink.setShutdownDraining(draining));
    },
    ...controls
  };
}

export function schedulerMetricsTelemetrySink(
  sink: RuntimeTelemetrySink | undefined
): RuntimeTelemetrySink | undefined {
  if (sink === undefined) {
    return undefined;
  }

  return {
    emit: async (event) => {
      if (event.name === "runtime.health.evaluated") {
        return;
      }

      if (event.name === "runtime.dependency.observed" && measuredDuration(event) === undefined) {
        return;
      }

      await sink.emit(event);
    }
  };
}

function measuredDuration(event: RuntimeTelemetryEvent): number | undefined {
  if (event.durationMs !== undefined && Number.isFinite(event.durationMs)) {
    return event.durationMs;
  }

  const durationMs = event.attributes?.durationMs;

  return typeof durationMs === "number" && Number.isFinite(durationMs) ? durationMs : undefined;
}

function safely(operation: () => void): void {
  try {
    operation();
  } catch {
    // Metric mutation is best effort and cannot alter service behavior.
  }
}
