/**
 * Rendering contracts of the Foldkit design-system port: the same classes,
 * data-slot hooks, native disabled semantics, labels, and ARIA wiring as the
 * shadcn components. Keyboard and focus behaviour is compared against Radix
 * in a real browser by docs/spikes/foldkit/parity/run-parity.mjs.
 */
import { Option, Schema } from "effect";
import { Update } from "foldkit";
import type { Html, HtmlBuilder } from "foldkit/html";
import { defineMessageUnion } from "foldkit/message";
import {
	Command,
	Mount,
	click,
	expect,
	given,
	label,
	role,
	scene,
	text,
	type,
} from "foldkit/scene";
import { modifyFields } from "foldkit/struct";
import { Dialog, Menu } from "@foldkit/ui";
import { describe, test } from "vitest";

import { buttonVariants } from "@/components/ui/button-variants";

import * as Badge from "./badge";
import * as Button from "./button";
import * as BifrostDialog from "./dialog";
import * as DropdownMenu from "./dropdownMenu";
import * as Input from "./input";

type Action = "Edit" | "Archive" | "Delete";
const ActionsMenu = Menu.create<Action>();

type Model = Readonly<{
	name: string;
	clicks: number;
	dialog: Dialog.Model;
	menu: Menu.Model;
	lastAction: string;
}>;

const Message = defineMessageUnion({
	ClickedSave: {},
	UpdatedName: { value: Schema.String },
	GotDialogMessage: { message: Dialog.Message },
	GotMenuMessage: { message: Menu.Message },
});
type Message = typeof Message.Type;

const foldMenu = Update.foldChild({
	update: ActionsMenu.update,
	read: (model: Model) => Option.some(model.menu),
	write: (model, nextMenu) => modifyFields(model, { menu: () => nextMenu }),
	toParentMessage: (message) => Message.GotMenuMessage({ message }),
	foldOutMessage: Menu.OutMessage.match<
		Update.Step<Model, Message>,
		Menu.OutMessage<Action>
	>({
		Selected:
			({ value }) =>
			(model) => ({
				model: modifyFields(model, { lastAction: () => value }),
			}),
	}),
});

const update = (
	model: Model,
	message: Message,
): Update.Return<Model, Message> =>
	Message.match<Update.Return<Model, Message>>(message, {
		ClickedSave: () => ({
			model: modifyFields(model, { clicks: (clicks) => clicks + 1 }),
		}),
		UpdatedName: ({ value }) => ({
			model: modifyFields(model, { name: () => value }),
		}),
		GotDialogMessage: () => ({ model }),
		GotMenuMessage: ({ message }) => foldMenu(model, message),
	});

const view = (model: Model, h: HtmlBuilder<Message>): Html =>
	h.div(
		[],
		[
			Button.view(
				{
					onClick: Message.ClickedSave(),
					content: [`Saved ${model.clicks}`],
				},
				h,
			),
			Button.view(
				{ variant: "outline", isDisabled: true, content: ["Publish"] },
				h,
			),
			Badge.view({ variant: "secondary", content: ["Draft"] }, h),
			Input.view(
				{
					id: "name",
					label: "Workflow Name",
					value: model.name,
					onInput: (value) => Message.UpdatedName({ value }),
				},
				h,
			),
			DropdownMenu.view(
				{
					menu: ActionsMenu,
					model: model.menu,
					toParentMessage: (message) =>
						Message.GotMenuMessage({ message }),
					items: ["Edit", "Archive", "Delete"],
					itemContent: (item) => [item],
					trigger: ["Workflow Actions"],
					isItemDisabled: (item) => item === "Archive",
					isItemDestructive: (item) => item === "Delete",
				},
				h,
			),
			h.p([h.Role("status")], [model.lastAction]),
			BifrostDialog.view(
				{
					model: model.dialog,
					toParentMessage: (message) =>
						Message.GotDialogMessage({ message }),
					title: "Delete Workflow?",
					description: "This permanently deletes the workflow.",
				},
				h,
			),
		],
	);

const initialModel: Model = {
	name: "Nightly Ticket Sync",
	clicks: 0,
	dialog: Dialog.init({ id: "delete-dialog" }),
	menu: Menu.init({ id: "actions-menu" }),
	lastAction: "None",
};

describe("Foldkit design-system port", () => {
	test("Button keeps the shadcn classes, slots, and native disabled", () => {
		scene(
			{ update, view },
			given(initialModel),
			expect(role("button", { name: "Saved 0" })).toHaveClass(
				buttonVariants({ variant: "default", size: "default" }).split(
					" ",
				)[0]!,
			),
			expect(role("button", { name: "Saved 0" })).toHaveAttr(
				"data-slot",
				"button",
			),
			expect(role("button", { name: "Publish" })).toBeDisabled(),
			expect(role("button", { name: "Publish" })).toHaveAttr(
				"data-variant",
				"outline",
			),
			click(role("button", { name: "Saved 0" })),
			expect(role("button", { name: "Saved 1" })).toExist(),
		);
	});

	test("Input is reachable by its label and Badge carries its variant", () => {
		scene(
			{ update, view },
			given(initialModel),
			expect(text("Draft")).toHaveAttr("data-variant", "secondary"),
			type(label("Workflow Name"), "Hourly Sync"),
			expect(label("Workflow Name")).toHaveValue("Hourly Sync"),
		);
	});

	test("DropdownMenu exposes menu-button ARIA and reports typed selections", () => {
		scene(
			{ update, view },
			given(initialModel),
			expect(role("button", { name: "Workflow Actions" })).toHaveAttr(
				"aria-haspopup",
				"menu",
			),
			expect(role("button", { name: "Workflow Actions" })).toHaveAttr(
				"aria-expanded",
				"false",
			),
			click(role("button", { name: "Workflow Actions" })),
			Command.resolve(
				Menu.FocusItems,
				Menu.Message.CompletedFocusItems(),
			),
			Mount.resolve(Menu.AnchorMenu, Menu.Message.CompletedAnchorMenu()),
			Mount.resolve(
				Menu.PortalMenuBackdrop,
				Menu.Message.CompletedPortalMenuBackdrop(),
			),
			expect(role("button", { name: "Workflow Actions" })).toHaveAttr(
				"aria-expanded",
				"true",
			),
			expect(role("menuitem", { name: "Archive" })).toHaveAttr(
				"aria-disabled",
				"true",
			),
			expect(role("menuitem", { name: "Delete" })).toHaveClass(
				"text-destructive",
			),
			click(role("menuitem", { name: "Edit" })),
			Command.resolve(
				Menu.FocusButton,
				Menu.Message.CompletedFocusButton(),
			),
			Mount.expectEnded(Menu.AnchorMenu, Menu.PortalMenuBackdrop),
			expect(role("status")).toHaveText("Edit"),
		);
	});

	test("Dialog renders the shadcn title and description slots when open", () => {
		const open = Dialog.open(initialModel.dialog);
		scene(
			{ update, view },
			given(modifyFields(initialModel, { dialog: () => open.model })),
			Mount.resolve(
				Dialog.AcquireResources,
				Dialog.Message.SucceededAcquireResources(),
			),
			expect(role("dialog", { name: "Delete Workflow?" })).toExist(),
			expect(text("This permanently deletes the workflow.")).toHaveAttr(
				"data-slot",
				"dialog-description",
			),
			expect(role("button", { name: "Close" })).toHaveAttr(
				"data-slot",
				"dialog-close",
			),
		);
	});
});
