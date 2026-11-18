import { cva } from "class-variance-authority";

/** Framework-agnostic toggle classes, shared by the React component and
 *  non-React renderers. */
export const toggleVariants = cva(
	"inline-flex items-center justify-center rounded-[var(--bf-radius-control)] text-sm font-medium ring-offset-background transition-[color,background-color,border-color,box-shadow] duration-[var(--bf-motion-feedback)] hover:bg-muted hover:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 data-[state=on]:bg-primary data-[state=on]:text-primary-foreground motion-reduce:transition-none",
	{
		variants: {
			variant: {
				default: "bg-transparent",
				outline:
					"border border-border/70 bg-transparent hover:bg-muted hover:text-foreground",
			},
			size: {
				default: "h-10 px-3",
				sm: "h-9 px-2.5",
				lg: "h-11 px-5",
			},
		},
		defaultVariants: {
			variant: "default",
			size: "default",
		},
	},
);
