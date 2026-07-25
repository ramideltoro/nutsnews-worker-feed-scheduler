export type SchedulerSkipReason =
  | "disabled"
  | "backoff"
  | "not_due";

export interface SchedulerFeedDefinition {
  readonly feedId: string;
  readonly feedUrl: string;
  readonly enabled: boolean;
  readonly cadenceMs: number;
  readonly priority: number;
  readonly shardIndex: number;
  readonly shardCount: number;
  readonly lastScheduledAt?: string;
  readonly disabledReason?: string;
  readonly backoffUntil?: string;
  readonly conditional?: Readonly<Record<string, string>>;
  readonly limits?: {
    readonly timeoutMs?: number;
    readonly maxItems?: number;
  };
}

export interface SchedulerWindow {
  readonly start: string;
  readonly end: string;
  readonly key: string;
}

export interface DueFeedDecision {
  readonly status: "due";
  readonly feed: SchedulerFeedDefinition;
  readonly window: SchedulerWindow;
  readonly idempotencyKey: string;
  readonly nextEligibleAt: string;
}

export interface SkippedFeedDecision {
  readonly status: "skipped";
  readonly reason: SchedulerSkipReason;
  readonly feed: SchedulerFeedDefinition;
  readonly window?: SchedulerWindow;
  readonly nextEligibleAt?: string;
}

export type FeedScheduleDecision = DueFeedDecision | SkippedFeedDecision;

export interface SelectDueFeedsResult {
  readonly decisions: readonly FeedScheduleDecision[];
  readonly due: readonly DueFeedDecision[];
  readonly skipped: readonly SkippedFeedDecision[];
}

export function selectDueFeeds(
  feeds: readonly SchedulerFeedDefinition[],
  now: Date,
  limit: number
): SelectDueFeedsResult {
  const decisions = feeds.map((feed) => evaluateFeedSchedule(feed, now));
  const due = decisions
    .filter((decision): decision is DueFeedDecision => decision.status === "due")
    .sort(compareDueFeeds)
    .slice(0, limit);
  const skipped = decisions.filter((decision): decision is SkippedFeedDecision => decision.status === "skipped");

  return {
    decisions,
    due,
    skipped
  };
}

export function evaluateFeedSchedule(feed: SchedulerFeedDefinition, now: Date): FeedScheduleDecision {
  if (!feed.enabled) {
    return {
      status: "skipped",
      reason: "disabled",
      feed
    };
  }

  const window = scheduleWindowFor(feed, now);
  const nextEligibleAt = nextEligibleAtFor(feed, now);

  if (feed.backoffUntil !== undefined && Date.parse(feed.backoffUntil) > now.getTime()) {
    return {
      status: "skipped",
      reason: "backoff",
      feed,
      window,
      nextEligibleAt: feed.backoffUntil
    };
  }

  if (Date.parse(nextEligibleAt) > now.getTime()) {
    return {
      status: "skipped",
      reason: "not_due",
      feed,
      window,
      nextEligibleAt
    };
  }

  return {
    status: "due",
    feed,
    window,
    idempotencyKey: idempotencyKeyFor(feed.feedId, window),
    nextEligibleAt
  };
}

export function scheduleWindowFor(feed: SchedulerFeedDefinition, now: Date): SchedulerWindow {
  if (!Number.isInteger(feed.cadenceMs) || feed.cadenceMs <= 0) {
    throw new RangeError("feed cadenceMs must be a positive integer.");
  }

  const startMs = Math.floor(now.getTime() / feed.cadenceMs) * feed.cadenceMs;
  const endMs = startMs + feed.cadenceMs;

  return {
    start: new Date(startMs).toISOString(),
    end: new Date(endMs).toISOString(),
    key: compactInstant(new Date(startMs))
  };
}

export function nextEligibleAtFor(feed: SchedulerFeedDefinition, now: Date): string {
  if (feed.lastScheduledAt === undefined) {
    return now.toISOString();
  }

  const parsed = Date.parse(feed.lastScheduledAt);

  if (Number.isNaN(parsed)) {
    return now.toISOString();
  }

  return new Date(parsed + feed.cadenceMs).toISOString();
}

export function idempotencyKeyFor(feedId: string, window: SchedulerWindow): string {
  return `scheduler:feed:${slugify(feedId)}:${window.key}`;
}

function compareDueFeeds(left: DueFeedDecision, right: DueFeedDecision): number {
  if (left.feed.priority !== right.feed.priority) {
    return right.feed.priority - left.feed.priority;
  }

  return left.feed.feedId.localeCompare(right.feed.feedId);
}

function compactInstant(value: Date): string {
  return value.toISOString().replace(/[-:.]/gu, "").replace("Z", "z").toLowerCase();
}

function slugify(value: string): string {
  const slug = value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._:/-]+/gu, "-");
  const trimmed = trimHyphens(slug);

  return trimmed.length > 0 ? trimmed.slice(0, 120) : "unknown";
}

function trimHyphens(value: string): string {
  let start = 0;
  let end = value.length;

  while (start < end && value[start] === "-") {
    start += 1;
  }

  while (end > start && value[end - 1] === "-") {
    end -= 1;
  }

  return value.slice(start, end);
}
