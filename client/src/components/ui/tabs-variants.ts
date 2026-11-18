import { cva } from "class-variance-authority";

/** Framework-agnostic tabs classes, shared by the React component and
 *  non-React renderers. */
export const tabsListVariants = cva(
	"group/tabs-list inline-flex w-fit items-center justify-center rounded-[var(--bf-radius-surface)] border border-border/70 bg-muted p-1 text-muted-foreground shadow-none group-data-horizontal/tabs:h-10 group-data-horizontal/tabs:overflow-y-hidden group-data-horizontal/tabs:data-[variant=line]:h-auto group-data-horizontal/tabs:data-[variant=line]:min-h-10 group-data-horizontal/tabs:[&.flex-wrap]:h-auto [&.flex-wrap]:gap-1 [&.flex-wrap>[data-slot=tabs-trigger]]:min-h-11 group-data-vertical/tabs:h-fit group-data-vertical/tabs:flex-col group-data-vertical/tabs:p-1 data-[variant=line]:rounded-none data-[variant=line]:border-0 group-data-horizontal/tabs:data-[variant=line]:border-b data-[variant=line]:bg-transparent data-[variant=line]:p-0",
	{
		variants: {
			variant: {
				default: "",
				line: "gap-1",
			},
		},
		defaultVariants: {
			variant: "default",
		},
	},
);
