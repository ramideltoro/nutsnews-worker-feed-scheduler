import type {
  PrometheusRuntimeTelemetrySink,
  RuntimeTelemetryFlusher,
  RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

export type SchedulerMetricsSink = PrometheusRuntimeTelemetrySink;

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

  return {
    allowedLabels: sink.allowedLabels,
    emit: async (event): Promise<void> => {
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
    setExpectedActive: (expected): void => {
      safely(() => sink.setExpectedActive(expected));
    },
    setLastSuccessTimestamp: (timestampSeconds): void => {
      safely(() => sink.setLastSuccessTimestamp(timestampSeconds));
    }
  };
}

function safely(operation: () => void): void {
  try {
    operation();
  } catch {
    // Metric mutation is best effort and cannot alter service behavior.
  }
}
