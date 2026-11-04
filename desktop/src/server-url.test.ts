import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { partitionFor, validateServerUrl } from "./server-url.js";

describe("validateServerUrl", () => {
	it("accepts https and normalizes to the origin", () => {
		assert.equal(
			validateServerUrl("https://bifrost.example.com/app?x=1"),
			"https://bifrost.example.com",
		);
	});

	it("trims whitespace", () => {
		assert.equal(
			validateServerUrl("  https://bifrost.example.com/  "),
			"https://bifrost.example.com",
		);
	});

	it("keeps non-default ports", () => {
		assert.equal(
			validateServerUrl("https://bifrost.example.com:8443"),
			"https://bifrost.example.com:8443",
		);
	});

	it("allows http only for localhost (local dev)", () => {
		assert.equal(validateServerUrl("http://localhost:3000"), "http://localhost:3000");
		assert.equal(validateServerUrl("http://localhost"), "http://localhost");
	});

	it("rejects http on any other host", () => {
		assert.equal(validateServerUrl("http://bifrost.example.com"), null);
		assert.equal(validateServerUrl("http://127.0.0.1:3000"), null);
	});

	it("rejects garbage", () => {
		assert.equal(validateServerUrl(""), null);
		assert.equal(validateServerUrl("not a url"), null);
		assert.equal(validateServerUrl("ftp://bifrost.example.com"), null);
	});
});

describe("partitionFor", () => {
	it("is stable and distinct per origin", () => {
		const a = partitionFor("https://one.example.com");
		assert.equal(a, partitionFor("https://one.example.com"));
		assert.notEqual(a, partitionFor("https://two.example.com"));
		assert.notEqual(a, partitionFor("http://localhost:3000"));
	});

	it("contains only partition-safe characters", () => {
		assert.match(partitionFor("https://bifrost.example.com:8443"), /^[A-Za-z0-9:-]+$/);
	});
});
