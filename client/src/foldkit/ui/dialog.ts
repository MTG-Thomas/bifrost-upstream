import type { Html, HtmlBuilder } from "foldkit/html";
import { Dialog } from "@foldkit/ui";
import { X } from "lucide";

import { buttonVariants } from "@/components/ui/button-variants";
import {
	dialogCloseButtonClassName,
	dialogContentClassName,
	dialogContentWithCloseButtonClassName,
	dialogDescriptionClassName,
	dialogFooterClassName,
	dialogHeaderClassName,
	dialogOverlayClassName,
	dialogTitleClassName,
} from "@/components/ui/dialog-variants";
import { cn } from "@/lib/utils";

import { icon } from "./icons";

/** Foldkit's enter/exit motion: `@foldkit/ui` Dialog marks the backdrop and
 *  panel `data-closed` while transitioning, where Radix uses keyframes. */
const OVERLAY_MOTION_CLASS =
	"transition-opacity duration-[var(--bf-motion-disclosure)] motion-reduce:transition-none data-[closed]:opacity-0";

const PANEL_MOTION_CLASS =
	"transition-[opacity,scale] duration-[var(--bf-motion-disclosure)] motion-reduce:transition-none data-[closed]:opacity-0 data-[closed]:scale-95";

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
									h.Class(
										cn(
											dialogOverlayClassName,
											OVERLAY_MOTION_CLASS,
										),
									),
								]),
								h.div(
									[
										...render.panel,
										h.DataAttribute(
											"slot",
											"dialog-content",
										),
										h.Class(
											cn(
												dialogContentClassName,
												PANEL_MOTION_CLASS,
												isCloseButtonShown &&
													dialogContentWithCloseButtonClassName,
												panelClassName,
											),
										),
									],
									[
										h.div(
											[
												h.DataAttribute(
													"slot",
													"dialog-header",
												),
												h.Class(dialogHeaderClassName),
											],
											[
												h.h2(
													[
														...render.title,
														h.DataAttribute(
															"slot",
															"dialog-title",
														),
														h.Class(
															dialogTitleClassName,
														),
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
																	dialogDescriptionClassName,
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
														h.Class(
															dialogFooterClassName,
														),
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
																dialogCloseButtonClassName,
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
