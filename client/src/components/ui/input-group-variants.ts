import { cva } from "class-variance-authority";

/** Framework-agnostic input-group classes, shared by the React component and
 *  non-React renderers. */
export const inputGroupAddonVariants = cva(
	"flex min-w-0 h-auto cursor-text items-center justify-center gap-2 py-1.5 text-sm font-medium text-muted-foreground select-none group-data-[disabled=true]/input-group:opacity-50 **:data-[slot=kbd]:rounded-[var(--bf-radius-control)] **:data-[slot=kbd]:bg-muted-foreground/10 **:data-[slot=kbd]:px-1.5 [&>svg:not([class*='size-'])]:size-4",
	{
		variants: {
			align: {
				"inline-start":
					"order-first pl-2 has-[>button]:ml-[-0.3rem] has-[>kbd]:ml-[-0.15rem]",
				"inline-end":
					"order-last pr-2 has-[>button]:mr-[-0.3rem] has-[>kbd]:mr-[-0.15rem]",
				"block-start":
					"order-first w-full flex-wrap justify-start px-2.5 pt-2 group-has-[>input]/input-group:pt-2 [.border-b]:pb-2",
				"block-end":
					"order-last w-full flex-wrap justify-start px-2.5 pb-2 group-has-[>input]/input-group:pb-2 [.border-t]:pt-2",
			},
		},
		defaultVariants: {
			align: "inline-start",
		},
	},
);

export const inputGroupButtonVariants = cva(
	"flex min-h-11 min-w-11 items-center gap-2 rounded-[var(--bf-radius-control)] text-sm shadow-none sm:min-h-0 sm:min-w-0",
	{
		variants: {
			size: {
				xs: "h-6 gap-1 rounded-[var(--bf-radius-control)] px-1.5 [&>svg:not([class*='size-'])]:size-3.5",
				sm: "",
				"icon-xs":
					"size-6 rounded-[var(--bf-radius-control)] p-0 has-[>svg]:p-0",
				"icon-sm": "size-8 p-0 has-[>svg]:p-0",
			},
		},
		defaultVariants: {
			size: "xs",
		},
	},
);
