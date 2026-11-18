import type { Attribute, Html, HtmlBuilder } from "foldkit/html";
import { Input as UiInput } from "@foldkit/ui";

import { cn } from "@/lib/utils";

/** The class string of `@/components/ui/input`. */
export const INPUT_CLASS =
	"h-11 w-full min-w-0 rounded-[var(--bf-radius-control)] border border-border/70 bg-background px-3 py-2 text-base transition-[color,box-shadow,border-color] duration-[var(--bf-motion-feedback)] motion-reduce:transition-none outline-none file:inline-flex file:h-6 file:border-0 file:bg-transparent file:text-sm file:font-medium file:text-foreground placeholder:text-muted-foreground hover:border-border focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/25 disabled:pointer-events-none disabled:cursor-not-allowed disabled:opacity-50 aria-invalid:border-destructive aria-invalid:ring-2 aria-invalid:ring-destructive/20 md:h-[var(--bf-control-height)] md:text-sm dark:aria-invalid:border-destructive/50 dark:aria-invalid:ring-destructive/40";

/** The class string of `@/components/ui/label`. */
export const LABEL_CLASS =
	"flex items-center gap-2 text-sm leading-none font-medium select-none group-data-[disabled=true]:pointer-events-none group-data-[disabled=true]:opacity-50 peer-disabled:cursor-not-allowed peer-disabled:opacity-50";

/**
 * Bifrost text input with its label. The label is required: pass
 * `isLabelVisible: false` for an icon-led field, which keeps the label for
 * assistive technology only. This replaces the React pattern of an
 * `aria-label` on a bare `<Input>`.
 */
export type ViewConfig<Message> = Readonly<{
	id: string;
	label: string;
	value: string;
	onInput: (value: string) => Message;
	isLabelVisible?: boolean;
	placeholder?: string;
	type?: string;
	isDisabled?: boolean;
	isInvalid?: boolean;
	className?: string;
	attributes?: ReadonlyArray<Attribute<Message>>;
	leading?: Html;
	trailing?: Html;
}>;

export const view = <Message>(
	{
		id,
		label,
		value,
		onInput,
		isLabelVisible = true,
		placeholder,
		type,
		isDisabled,
		isInvalid,
		className,
		attributes = [],
		leading,
		trailing,
	}: ViewConfig<Message>,
	h: HtmlBuilder<Message>,
): Html =>
	UiInput.view(
		{
			id,
			value,
			onInput,
			placeholder,
			type,
			isDisabled,
			isInvalid,
			toView: (inputAttributes) =>
				h.div(
					[
						h.Class(
							isLabelVisible
								? "grid min-w-0 gap-2"
								: "relative min-w-0",
						),
					],
					[
						h.label(
							[
								...inputAttributes.label,
								h.DataAttribute("slot", "label"),
								h.Class(
									isLabelVisible ? LABEL_CLASS : "sr-only",
								),
							],
							[label],
						),
						leading ?? h.empty,
						h.input([
							...inputAttributes.input,
							h.DataAttribute("slot", "input"),
							h.Class(cn(INPUT_CLASS, className)),
							...attributes,
						]),
						trailing ?? h.empty,
					],
				),
		},
		h,
	);
