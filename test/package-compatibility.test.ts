import { readFile } from "node:fs/promises";

import {
  CONTRACT_FIXTURE_SUITES,
  getContractCompatibilityEntry,
  getContractPackageMetadata
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  RUNTIME_ALLOWED_METRIC_LABELS,
  getRuntimePackageMetadata
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it
} from "vitest";

const CONTRACTS_PACKAGE = "@ramideltoro/nutsnews-worker-contracts";
const RUNTIME_PACKAGE = "@ramideltoro/nutsnews-worker-runtime";
const EXPECTED_VERSION = "1.0.0";

interface PackageManifest {
  readonly dependencies?: Readonly<Record<string, string>>;
  readonly overrides?: unknown;
}

interface LockPackage {
  readonly version?: string;
  readonly resolved?: string;
  readonly integrity?: string;
  readonly dependencies?: Readonly<Record<string, string>>;
}

interface PackageLock {
  readonly packages: Readonly<Record<string, LockPackage>>;
}

describe("Contracts 1 and Runtime 1 compatibility", () => {
  it("loads the exact published package metadata and conformance matrix entry", () => {
    const contracts = getContractPackageMetadata();
    const runtime = getRuntimePackageMetadata();
    const compatibility = getContractCompatibilityEntry(EXPECTED_VERSION);

    expect(contracts).toMatchObject({
      packageName: CONTRACTS_PACKAGE,
      packageVersion: EXPECTED_VERSION,
      status: "compatibility-conformance"
    });
    expect(contracts.availableContractSurfaces).toEqual([
      "rabbitmq-topology-and-message-envelope",
      "stage-payload-schemas-and-fixtures",
      "compatibility-and-conformance"
    ]);
    expect(compatibility).toMatchObject({
      version: EXPECTED_VERSION,
      status: "supported",
      fixtureSuites: [
        CONTRACT_FIXTURE_SUITES.bootstrap,
        CONTRACT_FIXTURE_SUITES.topologyEnvelope,
        CONTRACT_FIXTURE_SUITES.stagePayloads
      ]
    });

    expect(runtime).toMatchObject({
      packageName: RUNTIME_PACKAGE,
      packageVersion: EXPECTED_VERSION,
      status: "grafana-cloud-telemetry-primitives",
      contractsPackageName: CONTRACTS_PACKAGE,
      contractsPackageVersion: EXPECTED_VERSION
    });
    expect(runtime.availableRuntimeModules).toEqual(expect.arrayContaining([
      "prometheus-metrics",
      "health",
      "shutdown",
      "trace-context"
    ]));
    expect(RUNTIME_ALLOWED_METRIC_LABELS).toEqual(expect.arrayContaining([
      "revision",
      "deployment",
      "adapter",
      "probe",
      "check"
    ]));
  });

  it("pins authenticated GitHub Packages artifacts with integrity and no legacy override", async () => {
    const packageManifest = JSON.parse(
      await readFile(new URL("../package.json", import.meta.url), "utf8")
    ) as PackageManifest;
    const lock = JSON.parse(
      await readFile(new URL("../package-lock.json", import.meta.url), "utf8")
    ) as PackageLock;
    const npmrc = await readFile(new URL("../.npmrc", import.meta.url), "utf8");
    const contracts = lock.packages[`node_modules/${CONTRACTS_PACKAGE}`];
    const runtime = lock.packages[`node_modules/${RUNTIME_PACKAGE}`];

    expect(packageManifest.dependencies?.[CONTRACTS_PACKAGE]).toBe(EXPECTED_VERSION);
    expect(packageManifest.dependencies?.[RUNTIME_PACKAGE]).toBe(EXPECTED_VERSION);
    expect(packageManifest.overrides).toBeUndefined();

    for (const [name, entry] of [
      [CONTRACTS_PACKAGE, contracts],
      [RUNTIME_PACKAGE, runtime]
    ] as const) {
      expect(entry, `${name} lock entry`).toBeDefined();
      expect(entry?.version).toBe(EXPECTED_VERSION);
      expect(entry?.resolved).toMatch(
        new RegExp(
          `^https://npm\\.pkg\\.github\\.com/download/${name.replace("/", "\\/")}/1\\.0\\.0/[a-f0-9]{40}$`,
          "u"
        )
      );
      expect(entry?.integrity).toMatch(/^sha512-[A-Za-z0-9+/]+={0,2}$/u);
    }

    expect(runtime?.dependencies?.[CONTRACTS_PACKAGE]).toBe(EXPECTED_VERSION);
    expect(npmrc).toContain("@ramideltoro:registry=https://npm.pkg.github.com");
    expect(npmrc).toContain("//npm.pkg.github.com/:_authToken=${NODE_AUTH_TOKEN}");
    expect(npmrc).not.toMatch(/_authToken=(?!\$\{NODE_AUTH_TOKEN\})\S+/u);
  });
});
