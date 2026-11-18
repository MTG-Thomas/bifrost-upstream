import type { VariantProps } from "class-variance-authority";
import type { Attribute, Html, HtmlBuilder } from "foldkit/html";

import { badgeVariants } from "@/components/ui/badge-variants";
import { cn } from "@/lib/utils";

/** Bifrost Badge for Foldkit views, matching `@/components/ui/badge`. */
export type ViewConfig<Message> = Readonly<{
	content: ReadonlyArray<Html | string>;
	variant?: NonNullable<VariantProps<typeof badgeVariants>["variant"]>;
	className?: string;
	attributes?: ReadonlyArray<Attribute<Message>>;
}>;

export const view = <Message>(
	{
		content,
		variant = "default",
		className,
		attributes = [],
	}: ViewConfig<Message>,
	h: HtmlBuilder<Message>,
): Html =>
	h.span(
		[
			h.DataAttribute("slot", "badge"),
			h.DataAttribute("variant", variant),
			h.Class(cn(badgeVariants({ variant }), className)),
			...attributes,
		],
		content,
	);
