import type { VariantProps } from "class-variance-authority";
import { childAttributes, type Html, type HtmlBuilder } from "foldkit/html";
import { Menu } from "@foldkit/ui";

import { buttonVariants } from "@/components/ui/button-variants";
import {
	dropdownMenuContentClassName,
	dropdownMenuItemClassName,
	dropdownMenuLabelClassName,
	dropdownMenuSeparatorClassName,
} from "@/components/ui/dropdown-menu-variants";
import { cn } from "@/lib/utils";

// NOTE: Radix highlights an item with DOM focus, so the React component's
// state rules use `focus:`. Foldkit's Menu keeps focus on the menu and marks
// the highlighted item `data-active`, so the same rules use `data-[active]:`.
const CONTENT_MOTION_CLASS =
	"transition-[opacity,scale] duration-[var(--bf-motion-disclosure)] motion-reduce:transition-none data-[closed]:opacity-0 data-[closed]:scale-95";

const ITEM_STATE_CLASS =
	"data-[active]:bg-accent data-[active]:text-accent-foreground";

const DESTRUCTIVE_ITEM_CLASS =
	"text-destructive data-[active]:bg-destructive/10 data-[active]:text-destructive dark:data-[active]:bg-destructive/20 *:[svg]:text-destructive";

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
				h.Class(cn(dropdownMenuContentClassName, CONTENT_MOTION_CLASS)),
			]),
			backdropAttributes: childAttributes([
				h.Class("fixed inset-0 z-40"),
			]),
			attributes: childAttributes([h.Class("relative inline-block")]),
			itemToConfig: (item) => ({
				className: cn(
					dropdownMenuItemClassName,
					ITEM_STATE_CLASS,
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
									className: dropdownMenuLabelClassName,
								};
					}
				: undefined,
			separatorClassName: dropdownMenuSeparatorClassName,
		},
		toParentMessage,
	});
