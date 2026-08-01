import { readFile } from "node:fs/promises";

import {
  describe,
  expect,
  it
} from "vitest";

describe("scheduler image deployment contract", () => {
  it("uses liveness for container health independently of paging ownership", async () => {
    const dockerfile = await readFile(new URL("../Dockerfile", import.meta.url), "utf8");

    expect(dockerfile).toContain("fetch('http://127.0.0.1:8080/live')");
    expect(dockerfile).not.toContain("fetch('http://127.0.0.1:8080/ready')");
  });

  it("injects the immutable Git SHA into the image build revision", async () => {
    const workflow = await readFile(new URL("../.github/workflows/publish-container.yml", import.meta.url), "utf8");
    const dockerfile = await readFile(new URL("../Dockerfile", import.meta.url), "utf8");

    expect(workflow).toContain("NUTSNEWS_BUILD_REVISION=${{ github.sha }}");
    expect(dockerfile).toContain("NUTSNEWS_SCHEDULER_BUILD_REVISION=${NUTSNEWS_BUILD_REVISION}");
  });
});
