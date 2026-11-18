import type { VariantProps } from "class-variance-authority";
import type { Attribute, Html, HtmlBuilder } from "foldkit/html";
import { Button as UiButton } from "@foldkit/ui";

import { buttonVariants } from "@/components/ui/button-variants";
import { cn } from "@/lib/utils";

type Variants = VariantProps<typeof buttonVariants>;

/** Bifrost Button for Foldkit views. Same variants, sizes, data-slot
 *  attributes, and class merge as `@/components/ui/button`.
 *
 *  `@foldkit/ui` disables a button with `aria-disabled` alone, which keeps it
 *  focusable and leaves the `disabled:` variants unstyled. Bifrost's contract
 *  is the native `disabled` attribute, so it is added here. */
export type ViewConfig<Message> = Readonly<{
	content: ReadonlyArray<Html | string>;
	onClick?: Message;
	variant?: NonNullable<Variants["variant"]>;
	size?: NonNullable<Variants["size"]>;
	type?: "button" | "submit" | "reset";
	isDisabled?: boolean;
	className?: string;
	attributes?: ReadonlyArray<Attribute<Message>>;
}>;

export const view = <Message>(
	{
		content,
		onClick,
		variant = "default",
		size = "default",
		type = "button",
		isDisabled = false,
		className,
		attributes = [],
	}: ViewConfig<Message>,
	h: HtmlBuilder<Message>,
): Html =>
	UiButton.view(
		{
			onClick,
			type,
			isDisabled,
			toView: (buttonAttributes) =>
				h.button(
					[
						...buttonAttributes.button,
						h.DataAttribute("slot", "button"),
						h.DataAttribute("variant", variant),
						h.DataAttribute("size", size),
						...(isDisabled ? [h.Disabled(true)] : []),
						h.Class(
							cn(buttonVariants({ variant, size, className })),
						),
						...attributes,
					],
					content,
				),
		},
		h,
	);
