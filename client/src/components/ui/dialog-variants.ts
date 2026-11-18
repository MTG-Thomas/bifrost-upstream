/** Framework-agnostic dialog classes, shared by the React component and non-React
 *  renderers. Structure only:
 *  each renderer adds its own enter/exit motion, because Radix animates with
 *  `data-open`/`data-closed` keyframes and Foldkit with `data-closed`
 *  transitions. */
export const dialogOverlayClassName =
	"fixed inset-0 isolate z-50 bg-black/30 supports-backdrop-filter:backdrop-blur-sm";

export const dialogContentClassName =
	"fixed top-1/2 left-1/2 z-50 grid max-h-[90dvh] w-[calc(100%-2rem)] max-w-md -translate-x-1/2 -translate-y-1/2 gap-6 overflow-y-auto rounded-[var(--bf-radius-feature)] border border-border/70 bg-popover p-6 text-sm text-popover-foreground shadow-xl ring-1 ring-foreground/5 outline-none dark:ring-foreground/10";

/** Keeps a long title clear of the corner close button. */
export const dialogContentWithCloseButtonClassName =
	"[&_[data-slot=dialog-header]]:pr-10";

export const dialogCloseButtonClassName =
	"absolute top-3 right-3 h-11 w-11 lg:h-8 lg:w-8";

export const dialogHeaderClassName =
	"flex min-w-0 flex-col gap-1.5 [overflow-wrap:anywhere]";

export const dialogFooterClassName =
	"flex flex-col-reverse gap-2 sm:flex-row sm:justify-end";

export const dialogTitleClassName =
	"text-base leading-snug font-medium [overflow-wrap:anywhere]";

export const dialogDescriptionClassName =
	"text-sm [overflow-wrap:anywhere] text-muted-foreground *:[a]:underline *:[a]:underline-offset-3 *:[a]:hover:text-foreground";
