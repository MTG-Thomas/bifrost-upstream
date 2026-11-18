import type { Html, HtmlBuilder } from "foldkit/html";
import { Dialog } from "@foldkit/ui";
import { X } from "lucide";

import { buttonVariants } from "@/components/ui/button-variants";
import { cn } from "@/lib/utils";

import { icon } from "./icons";

/** Overlay of `@/components/ui/dialog`, with Foldkit's `data-closed`
 *  transition hook in place of Radix's `data-open`/`data-closed` keyframes. */
const OVERLAY_CLASS =
	"fixed inset-0 isolate z-50 bg-black/30 supports-backdrop-filter:backdrop-blur-sm transition-opacity duration-[var(--bf-motion-disclosure)] motion-reduce:transition-none data-[closed]:opacity-0";

const PANEL_CLASS =
	"fixed top-1/2 left-1/2 z-50 grid max-h-[90dvh] w-[calc(100%-2rem)] max-w-md -translate-x-1/2 -translate-y-1/2 gap-6 overflow-y-auto rounded-[var(--bf-radius-feature)] border border-border/70 bg-popover p-6 text-sm text-popover-foreground shadow-xl ring-1 ring-foreground/5 outline-none dark:ring-foreground/10 transition-[opacity,scale] duration-[var(--bf-motion-disclosure)] motion-reduce:transition-none data-[closed]:opacity-0 data-[closed]:scale-95";

const HEADER_CLASS = "flex min-w-0 flex-col gap-1.5 [overflow-wrap:anywhere]";
const FOOTER_CLASS = "flex flex-col-reverse gap-2 sm:flex-row sm:justify-end";
const TITLE_CLASS =
	"text-base leading-snug font-medium [overflow-wrap:anywhere]";
const DESCRIPTION_CLASS =
	"text-sm [overflow-wrap:anywhere] text-muted-foreground *:[a]:underline *:[a]:underline-offset-3 *:[a]:hover:text-foreground";

export type RenderInfo = Dialog.RenderInfo;

/**
 * Bifrost modal dialog: `@foldkit/ui` Dialog (native `<dialog>`, focus
 * trapping, Escape, backdrop dismissal) dressed as `@/components/ui/dialog`.
 * The parent owns the `Dialog.Model` and folds `Dialog.update` like any
 * other Submodel. `footer` receives the RenderInfo so its buttons can spread
 * `render.closeButton` (Cancel) or dispatch their own Messages (confirm).
 */
export type ViewConfig<ParentMessage> = Readonly<{
	model: Dialog.Model;
	toParentMessage: (message: Dialog.Message) => ParentMessage;
	title: string;
	description?: string;
	body?: (render: RenderInfo) => ReadonlyArray<Html>;
	footer?: (render: RenderInfo) => ReadonlyArray<Html>;
	isCloseButtonShown?: boolean;
	panelClassName?: string;
}>;

export const view = <ParentMessage>(
	{
		model,
		toParentMessage,
		title,
		description,
		body,
		footer,
		isCloseButtonShown = true,
		panelClassName,
	}: ViewConfig<ParentMessage>,
	h: HtmlBuilder<ParentMessage>,
): Html =>
	h.submodel({
		slotId: model.id,
		model,
		view: Dialog.view,
		viewInputs: {
			hasDescription: description !== undefined,
			toView: (render) =>
				h.dialog(
					[...render.dialog, h.DataAttribute("slot", "dialog")],
					render.isVisible
						? [
								h.div([
									...render.backdrop,
									h.DataAttribute("slot", "dialog-overlay"),
									h.Class(OVERLAY_CLASS),
								]),
								h.div(
									[
										...render.panel,
										h.DataAttribute(
											"slot",
											"dialog-content",
										),
										h.Class(
											cn(PANEL_CLASS, panelClassName),
										),
									],
									[
										h.div(
											[
												h.DataAttribute(
													"slot",
													"dialog-header",
												),
												h.Class(HEADER_CLASS),
											],
											[
												h.h2(
													[
														...render.title,
														h.DataAttribute(
															"slot",
															"dialog-title",
														),
														h.Class(TITLE_CLASS),
													],
													[title],
												),
												description === undefined
													? h.empty
													: h.p(
															[
																...render.description,
																h.DataAttribute(
																	"slot",
																	"dialog-description",
																),
																h.Class(
																	DESCRIPTION_CLASS,
																),
															],
															[description],
														),
											],
										),
										...(body ? body(render) : []),
										footer
											? h.div(
													[
														h.DataAttribute(
															"slot",
															"dialog-footer",
														),
														h.Class(FOOTER_CLASS),
													],
													footer(render),
												)
											: h.empty,
										isCloseButtonShown
											? h.button(
													[
														...render.closeButton,
														h.DataAttribute(
															"slot",
															"dialog-close",
														),
														h.Class(
															cn(
																buttonVariants({
																	variant:
																		"outline",
																	size: "icon-sm",
																}),
																"absolute top-3 right-3 h-11 w-11 lg:h-8 lg:w-8",
															),
														),
													],
													[
														icon(X, "", h),
														h.span(
															[
																h.Class(
																	"sr-only",
																),
															],
															["Close"],
														),
													],
												)
											: h.empty,
									],
								),
							]
						: [],
				),
		},
		toParentMessage,
	});
