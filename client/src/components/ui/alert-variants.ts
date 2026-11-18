import { cva } from "class-variance-authority";

/** Framework-agnostic alert classes, shared by the React component and
 *  non-React renderers. */
export const alertVariants = cva(
	"group/alert relative grid w-full gap-0.5 min-w-0 rounded-[var(--bf-radius-surface)] border border-border px-4 py-3 text-left text-sm has-data-[slot=alert-action]:relative has-data-[slot=alert-action]:pr-18 has-[>svg]:grid-cols-[auto_1fr] has-[>svg]:gap-x-2.5 *:[svg]:row-span-2 *:[svg]:translate-y-0.5 *:[svg]:text-current *:[svg:not([class*='size-'])]:size-4",
	{
		variants: {
			variant: {
				default: "bg-card text-card-foreground",
				destructive:
					"bg-card border-destructive/30 text-destructive *:data-[slot=alert-description]:text-foreground *:[svg]:text-current",
			},
		},
		defaultVariants: {
			variant: "default",
		},
	},
);
