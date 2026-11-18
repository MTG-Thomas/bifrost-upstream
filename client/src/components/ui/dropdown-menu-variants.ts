/** Framework-agnostic dropdown menu classes, shared by the React component and non-React
 *  renderers. Structure only:
 *  Radix highlights an item with DOM focus (`focus:`) and positions content
 *  through `--radix-*` variables, Foldkit marks the item `data-active` and
 *  anchors with floating-ui, so each renderer adds those classes itself. */
export const dropdownMenuContentClassName =
	"z-50 w-max max-w-[calc(100vw-2rem)] min-w-32 overflow-x-hidden overflow-y-auto rounded-[var(--bf-radius-surface)] border border-border/70 bg-popover p-1 text-popover-foreground shadow-lg ring-1 ring-foreground/5 dark:ring-foreground/10";

export const dropdownMenuItemClassName =
	"group/dropdown-menu-item relative flex min-h-7 cursor-default items-center gap-2 rounded-[var(--bf-radius-surface)] px-2 py-1.5 text-sm outline-hidden select-none data-inset:pl-7 data-disabled:pointer-events-none data-disabled:opacity-50 [&_svg]:pointer-events-none [&_svg]:shrink-0 [&_svg:not([class*='size-'])]:size-4";

export const dropdownMenuLabelClassName =
	"px-2 py-1 text-xs text-muted-foreground data-inset:pl-7";

export const dropdownMenuSeparatorClassName = "-mx-1 my-1 h-px bg-border/70";
