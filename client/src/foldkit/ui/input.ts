import type { Attribute, Html, HtmlBuilder } from "foldkit/html";
import { Input as UiInput } from "@foldkit/ui";

import { inputClassName } from "@/components/ui/input-variants";
import { labelClassName } from "@/components/ui/label-variants";
import { cn } from "@/lib/utils";

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
									isLabelVisible ? labelClassName : "sr-only",
								),
							],
							[label],
						),
						leading ?? h.empty,
						h.input([
							...inputAttributes.input,
							h.DataAttribute("slot", "input"),
							h.Class(cn(inputClassName, className)),
							...attributes,
						]),
						trailing ?? h.empty,
					],
				),
		},
		h,
	);
