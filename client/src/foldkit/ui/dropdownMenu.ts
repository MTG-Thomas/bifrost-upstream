import type { VariantProps } from "class-variance-authority";
import { childAttributes, type Html, type HtmlBuilder } from "foldkit/html";
import { Menu } from "@foldkit/ui";

import { buttonVariants } from "@/components/ui/button-variants";
import { cn } from "@/lib/utils";

// NOTE: Radix styles the highlighted item through `:focus`; Foldkit's Menu
// keeps focus on the menu and marks the highlighted item `data-active`, so
// every `focus:` rule from `@/components/ui/dropdown-menu` maps to
// `data-[active]:` here.
const CONTENT_CLASS =
	"z-50 w-max max-w-[calc(100vw-2rem)] min-w-32 overflow-x-hidden overflow-y-auto rounded-[var(--bf-radius-surface)] border border-border/70 bg-popover p-1 text-popover-foreground shadow-lg ring-1 ring-foreground/5 outline-none dark:ring-foreground/10 transition-[opacity,scale] duration-[var(--bf-motion-disclosure)] motion-reduce:transition-none data-[closed]:opacity-0 data-[closed]:scale-95";

const ITEM_CLASS =
	"relative flex min-h-7 cursor-default items-center gap-2 rounded-[var(--bf-radius-surface)] px-2 py-1.5 text-sm outline-hidden select-none data-[active]:bg-accent data-[active]:text-accent-foreground data-[disabled]:pointer-events-none data-[disabled]:opacity-50 [&_svg]:pointer-events-none [&_svg]:shrink-0 [&_svg:not([class*='size-'])]:size-4";

const DESTRUCTIVE_ITEM_CLASS =
	"text-destructive data-[active]:bg-destructive/10 data-[active]:text-destructive dark:data-[active]:bg-destructive/20 *:[svg]:text-destructive";

const LABEL_CLASS = "px-2 py-1 text-xs text-muted-foreground";
const SEPARATOR_CLASS = "-mx-1 my-1 h-px bg-border";

type ButtonVariants = VariantProps<typeof buttonVariants>;

/**
 * Bifrost dropdown menu: `@foldkit/ui` Menu (roving highlight, typeahead,
 * disabled items, groups, anchored positioning) dressed as
 * `@/components/ui/dropdown-menu`. Items are a string union, so a selection
 * arrives in the parent as a typed `Menu.OutMessage<Item>`.
 *
 * Group items with `itemGroupKey`; a separator renders between groups and
 * `groupLabel` adds a `DropdownMenuLabel`-style heading.
 */
export type ViewConfig<Item extends string, ParentMessage> = Readonly<{
	menu: Menu.Bundle<Item>;
	model: Menu.Model;
	toParentMessage: (message: Menu.Message) => ParentMessage;
	items: ReadonlyArray<Item>;
	itemContent: (item: Item) => ReadonlyArray<Html | string>;
	trigger: ReadonlyArray<Html | string>;
	triggerLabel?: string;
	triggerVariant?: NonNullable<ButtonVariants["variant"]>;
	triggerSize?: NonNullable<ButtonVariants["size"]>;
	isItemDisabled?: (item: Item) => boolean;
	isItemDestructive?: (item: Item) => boolean;
	itemGroupKey?: (item: Item) => string;
	groupLabel?: (groupKey: string) => string | undefined;
	align?: "start" | "end";
}>;

export const view = <Item extends string, ParentMessage>(
	{
		menu,
		model,
		toParentMessage,
		items,
		itemContent,
		trigger,
		triggerLabel,
		triggerVariant = "outline",
		triggerSize = "default",
		isItemDisabled,
		isItemDestructive = () => false,
		itemGroupKey,
		groupLabel,
		align = "start",
	}: ViewConfig<Item, ParentMessage>,
	h: HtmlBuilder<ParentMessage>,
): Html =>
	h.submodel({
		slotId: model.id,
		model,
		view: menu.view,
		viewInputs: {
			items,
			anchor: {
				placement: align === "end" ? "bottom-end" : "bottom-start",
				gap: 4,
				padding: 8,
			},
			ariaLabel: triggerLabel,
			buttonContent: h.span([h.Class("contents")], trigger),
			buttonAttributes: childAttributes([
				h.DataAttribute("slot", "dropdown-menu-trigger"),
				h.Class(
					buttonVariants({
						variant: triggerVariant,
						size: triggerSize,
					}),
				),
			]),
			itemsAttributes: childAttributes([
				h.DataAttribute("slot", "dropdown-menu-content"),
				h.Class(CONTENT_CLASS),
			]),
			backdropAttributes: childAttributes([
				h.Class("fixed inset-0 z-40"),
			]),
			attributes: childAttributes([h.Class("relative inline-block")]),
			itemToConfig: (item) => ({
				className: cn(
					ITEM_CLASS,
					isItemDestructive(item) && DESTRUCTIVE_ITEM_CLASS,
				),
				content: h.span([h.Class("contents")], itemContent(item)),
			}),
			isItemDisabled: isItemDisabled
				? (item) => isItemDisabled(item)
				: undefined,
			itemGroupKey: itemGroupKey
				? (item) => itemGroupKey(item)
				: undefined,
			groupToHeading: groupLabel
				? (groupKey) => {
						const label = groupLabel(groupKey);
						return label === undefined
							? undefined
							: {
									content: h.span([], [label]),
									className: LABEL_CLASS,
								};
					}
				: undefined,
			separatorClassName: SEPARATOR_CLASS,
		},
		toParentMessage,
	});
