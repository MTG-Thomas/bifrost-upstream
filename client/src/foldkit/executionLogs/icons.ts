import type { Html, HtmlBuilder } from "foldkit/html";

// NOTE: lucide-react ships React components only, so the Foldkit view cannot
// reuse them. These are the same lucide v1 geometries (ISC licence), drawn
// with Foldkit's SVG builder.

type IconShape =
	| Readonly<{ kind: "Path"; d: string }>
	| Readonly<{ kind: "Circle"; cx: string; cy: string; r: string }>
	| Readonly<{
			kind: "Rect";
			x: string;
			y: string;
			width: string;
			height: string;
			rx: string;
	  }>;

const path = (d: string): IconShape => ({ kind: "Path", d });

export const ICONS = {
	search: [
		path("m21 21-4.34-4.34"),
		{ kind: "Circle", cx: "11", cy: "11", r: "8" },
	],
	x: [path("M18 6 6 18"), path("m6 6 12 12")],
	check: [path("M20 6 9 17l-5-5")],
	download: [
		path("M12 15V3"),
		path("M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"),
		path("m7 10 5 5 5-5"),
	],
	arrowDown: [path("M12 5v14"), path("m19 12-7 7-7-7")],
	copy: [
		{ kind: "Rect", x: "8", y: "8", width: "14", height: "14", rx: "2" },
		path("M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2"),
	],
} satisfies Record<string, ReadonlyArray<IconShape>>;

export type IconName = keyof typeof ICONS;

/** Renders a decorative lucide icon; the owning control carries the label. */
export const icon = <Message>(
	name: IconName,
	className: string,
	h: HtmlBuilder<Message>,
): Html =>
	h.svg(
		[
			h.Class(className),
			h.ViewBox("0 0 24 24"),
			h.Fill("none"),
			h.Stroke("currentColor"),
			h.StrokeWidth("2"),
			h.StrokeLinecap("round"),
			h.StrokeLinejoin("round"),
			h.AriaHidden(true),
		],
		ICONS[name].map((shape: IconShape) => {
			switch (shape.kind) {
				case "Path":
					return h.path([h.D(shape.d)], []);
				case "Circle":
					return h.circle(
						[h.Cx(shape.cx), h.Cy(shape.cy), h.R(shape.r)],
						[],
					);
				case "Rect":
					return h.rect(
						[
							h.X(shape.x),
							h.Y(shape.y),
							h.Width(shape.width),
							h.Height(shape.height),
							h.Rx(shape.rx),
						],
						[],
					);
			}
		}),
	);
