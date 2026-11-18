// Drives the React (shadcn/Radix) and Foldkit galleries through identical
// keyboard and pointer scenarios in Chromium and prints what each one did.
// Usage: node run-parity.mjs parity-dist [path/to/axe.min.js]
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { chromium } from "playwright";

const [dist, axePath] = process.argv.slice(2);
const server = http
  .createServer((req, res) => {
    const file = path.join(
      dist,
      req.url.split("?")[0] === "/" ? "index.html" : req.url.split("?")[0],
    );
    if (!fs.existsSync(file)) {
      res.writeHead(404);
      return res.end();
    }
    const type = file.endsWith(".js")
      ? "text/javascript"
      : file.endsWith(".css")
        ? "text/css"
        : "text/html";
    res.writeHead(200, { "content-type": type });
    fs.createReadStream(file).pipe(res);
  })
  .listen(0);
const base = `http://127.0.0.1:${server.address().port}/`;

const settle = (page) => page.waitForTimeout(700);

/** Foldkit renders on the next animation frame and Radix synchronously, so
 *  every interaction waits two frames before anything is read. */
const frames = (page) =>
  page.evaluate(
    () =>
      new Promise((resolve) =>
        requestAnimationFrame(() => requestAnimationFrame(resolve)),
      ),
  );

/** The focused element, described the way assistive tech would see it. */
const focused = (page) =>
  page.evaluate(() => {
    const element = document.activeElement;
    if (!element || element === document.body) return "body";
    const role = element.getAttribute("role") ?? element.tagName.toLowerCase();
    const name =
      element.getAttribute("aria-label") ??
      (element.labels?.[0]?.textContent ?? element.textContent ?? "")
        .trim()
        .slice(0, 40);
    return `${role}:${name}`;
  });

/** The highlighted menu item, whether the menu moves DOM focus (Radix) or
 *  marks the item while focus stays on the menu (Foldkit). */
const highlighted = (page) =>
  page.evaluate(() => {
    const item =
      document.querySelector('[role="menuitem"][data-highlighted]') ??
      document.querySelector('[role="menuitem"][data-active]') ??
      (document.activeElement?.getAttribute("role") === "menuitem"
        ? document.activeElement
        : null);
    return item ? item.textContent.trim() : "none";
  });

const press = async (page, key) => {
  await page.keyboard.press(key);
  await frames(page);
};

const dialogOpen = (page) =>
  page
    .getByRole("dialog")
    .isVisible()
    .catch(() => false);
const menuOpen = (page) =>
  page
    .getByRole("menu")
    .isVisible()
    .catch(() => false);
const status = (page) => page.getByTestId("last-action").textContent();

const focusInsideDialog = (page) =>
  page.evaluate(() => {
    const dialog = document.querySelector('[role="dialog"], dialog[open]');
    return Boolean(dialog && dialog.contains(document.activeElement));
  });

const axe = async (page) => {
  if (!axePath) return "skipped";
  await page.addScriptTag({ path: axePath });
  const result = await page.evaluate(async () => {
    const run = await window.axe.run(document, { resultTypes: ["violations"] });
    return run.violations.map(
      (violation) => `${violation.id}(${violation.nodes.length})`,
    );
  });
  return result.length === 0 ? "none" : result.join(" ");
};

async function scenarios(renderer) {
  const browser = await chromium.launch();
  const page = await browser.newPage({
    viewport: { width: 1280, height: 900 },
  });
  const errors = [];
  page.on("pageerror", (error) => errors.push(String(error)));
  page.on(
    "console",
    (message) => message.type() === "error" && errors.push(message.text()),
  );
  await page.goto(`${base}?renderer=${renderer}`);
  await page.getByRole("button", { name: "Delete Workflow" }).waitFor();
  const results = {};
  page.setDefaultTimeout(4000);
  const record = (key, value) => (results[key] = value);
  const check = async (key, read) => {
    try {
      results[key] = await read();
    } catch (error) {
      results[key] =
        `ERROR ${String(error.message).split("\n")[0].slice(0, 90)}`;
    }
  };
  const trigger = page.getByRole("button", { name: "Delete Workflow" });
  const menuTrigger = page.getByRole("button", { name: "Workflow Actions" });

  try {
    // Page-level semantics
    const tabOrder = [];
    await page.locator("body").click({ position: { x: 1, y: 1 } });
    for (let index = 0; index < 7; index++) {
      await press(page, "Tab");
      tabOrder.push(await focused(page));
    }
    record("page.tabOrder", tabOrder.join(" → "));
    await check("input.labelled", () =>
      page.getByLabel("Workflow Name").inputValue(),
    );
    record(
      "button.disabledSkipped",
      !tabOrder.some((entry) => entry.includes("Publish")),
    );
    await check("axe.closed", () => axe(page));

    // Dialog
    await trigger.focus();
    await press(page, "Enter");
    await settle(page);
    await check("dialog.opensWithEnter", () => dialogOpen(page));
    await check("dialog.name", () =>
      page
        .getByRole("dialog", { name: "Delete Workflow?" })
        .isVisible()
        .catch(() => false),
    );
    await check("dialog.description", () =>
      page
        .getByRole("dialog", {
          description:
            "This permanently deletes the workflow and its run history.",
        })
        .isVisible()
        .catch(() => false),
    );
    await check("dialog.initialFocus", () => focused(page));
    await check("axe.dialogOpen", () => axe(page));
    let trapped = true;
    const tabbed = [];
    for (let index = 0; index < 6; index++) {
      await press(page, "Tab");
      trapped = trapped && (await focusInsideDialog(page));
      tabbed.push(await focused(page));
    }
    record("dialog.tabCycle", tabbed.join(" → "));
    record("dialog.tabTrapped", trapped);
    await press(page, "Shift+Tab");
    await check("dialog.shiftTabTrapped", () => focusInsideDialog(page));
    await page.getByLabel("Type the Workflow Name").fill("Nightly");
    await check("dialog.inputTyping", () =>
      page.getByLabel("Type the Workflow Name").inputValue(),
    );
    await press(page, "Escape");
    await settle(page);
    record("dialog.escapeCloses", !(await dialogOpen(page)));
    await check("dialog.escapeReturnsFocus", () => focused(page));

    await trigger.click();
    await settle(page);
    await page.getByRole("button", { name: "Cancel" }).last().click();
    await settle(page);
    record("dialog.cancelCloses", !(await dialogOpen(page)));
    await check("dialog.cancelReturnsFocus", () => focused(page));

    await trigger.click();
    await settle(page);
    await page.mouse.click(20, 880);
    await settle(page);
    record("dialog.backdropCloses", !(await dialogOpen(page)));

    await trigger.click();
    await settle(page);
    await page.getByRole("button", { name: "Close" }).click();
    await settle(page);
    record("dialog.closeButtonCloses", !(await dialogOpen(page)));

    await trigger.click();
    await settle(page);
    await check("dialog.reopenResetsInput", () =>
      page.getByLabel("Type the Workflow Name").inputValue(),
    );
    await page.getByRole("button", { name: "Delete", exact: true }).click();
    await settle(page);
    await check(
      "dialog.confirmClosesAndActs",
      async () => `${!(await dialogOpen(page))} / ${await status(page)}`,
    );

    // Dropdown menu
    await check("menu.triggerHasPopup", () =>
      menuTrigger.getAttribute("aria-haspopup"),
    );
    await check("menu.triggerExpandedClosed", () =>
      menuTrigger.getAttribute("aria-expanded"),
    );
    await menuTrigger.focus();
    await press(page, "Enter");
    await settle(page);
    await check("menu.opensWithEnter", () => menuOpen(page));
    await check("menu.triggerExpandedOpen", () =>
      page
        .locator('[data-slot="dropdown-menu-trigger"]')
        .getAttribute("aria-expanded"),
    );
    await check(
      "menu.triggerInAccessibilityTree",
      async () =>
        (await page.getByRole("button", { name: "Workflow Actions" }).count()) >
        0,
    );
    await check("menu.initialHighlight", () => highlighted(page));
    await check("menu.focusModel", () => focused(page));
    await check("axe.menuOpen", () => axe(page));
    const arrowWalk = [];
    for (let index = 0; index < 4; index++) {
      await press(page, "ArrowDown");
      arrowWalk.push(await highlighted(page));
    }
    record("menu.arrowDownWalk", arrowWalk.join(" → "));
    await press(page, "Home");
    await check("menu.home", () => highlighted(page));
    await press(page, "End");
    await check("menu.end", () => highlighted(page));
    await press(page, "ArrowUp");
    await check("menu.arrowUp", () => highlighted(page));
    await press(page, "Home");
    await page.keyboard.type("du", { delay: 30 });
    await frames(page);
    await check("menu.typeahead", () => highlighted(page));
    await check("menu.groupLabel", () =>
      page.getByText("Danger Zone").isVisible(),
    );
    await check("menu.separator", () => page.getByRole("separator").count());
    await check("menu.disabledAria", () =>
      page
        .getByRole("menuitem", { name: "Archive" })
        .getAttribute("aria-disabled"),
    );
    await press(page, "Enter");
    await settle(page);
    await check(
      "menu.enterSelects",
      async () => `${!(await menuOpen(page))} / ${await status(page)}`,
    );
    await check("menu.selectReturnsFocus", () => focused(page));

    await menuTrigger.focus();
    await press(page, "ArrowDown");
    await settle(page);
    await check(
      "menu.opensWithArrowDown",
      async () => `${await menuOpen(page)} / ${await highlighted(page)}`,
    );
    await press(page, "Escape");
    await settle(page);
    record("menu.escapeCloses", !(await menuOpen(page)));
    await check("menu.escapeReturnsFocus", () => focused(page));

    await menuTrigger.click();
    await settle(page);
    await check("menu.opensWithClick", () => menuOpen(page));
    await page
      .getByRole("menuitem", { name: "Archive" })
      .click({ force: true });
    await settle(page);
    await check(
      "menu.disabledClickIgnored",
      async () => `${await menuOpen(page)} / ${await status(page)}`,
    );
    await page.getByRole("menuitem", { name: "Edit" }).click();
    await settle(page);
    await check(
      "menu.clickSelects",
      async () => `${!(await menuOpen(page))} / ${await status(page)}`,
    );

    await menuTrigger.click();
    await settle(page);
    await page.mouse.click(1200, 850);
    await settle(page);
    record("menu.outsideClickCloses", !(await menuOpen(page)));

    await menuTrigger.focus();
    await press(page, "Enter");
    await settle(page);
    await press(page, "Tab");
    await settle(page);
    await check(
      "menu.tabWhileOpen",
      async () =>
        `${(await menuOpen(page)) ? "open" : "closed"} / ${await focused(page)}`,
    );
  } catch (error) {
    record("aborted", String(error.message).split("\n")[0]);
  }
  record(
    "pageErrors",
    errors.length === 0 ? "none" : errors.join(" | ").slice(0, 200),
  );
  await browser.close();
  return results;
}

const react = await scenarios("react");
const foldkit = await scenarios("foldkit");
server.close();
const rows = [...new Set([...Object.keys(react), ...Object.keys(foldkit)])].map(
  (key) => ({
    key,
    react: String(react[key]),
    foldkit: String(foldkit[key]),
  }),
);
for (const row of rows) {
  const marker = row.react === row.foldkit ? "same" : "DIFF";
  console.log(
    `${marker}\t${row.key}\n\treact:   ${row.react}\n\tfoldkit: ${row.foldkit}`,
  );
}
console.log(
  `\n${rows.filter((row) => row.react === row.foldkit).length}/${rows.length} observations identical`,
);
