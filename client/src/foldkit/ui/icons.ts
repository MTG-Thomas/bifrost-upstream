import type { Html, HtmlBuilder } from "foldkit/html";
import type { IconNode } from "lucide";

const SHAPE_TAGS = [
	"path",
	"circle",
	"rect",
	"line",
	"ellipse",
	"polyline",
	"polygon",
] as const;
type ShapeTag = (typeof SHAPE_TAGS)[number];

const isShapeTag = (tag: string): tag is ShapeTag =>
	SHAPE_TAGS.some((shapeTag) => shapeTag === tag);

/**
 * Renders any lucide icon in a Foldkit view. `lucide` is the framework-free
 * build of the same icon set (and version) as `lucide-react`, exporting each
 * icon as plain `[tag, attributes]` data:
 *
 *   import { Copy } from "lucide";
 *   icon(Copy, "size-4", h)
 *
 * Icons are decorative; the owning control carries the accessible name.
 */
export const icon = <Message>(
	node: IconNode,
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
		node.flatMap(([tag, attributes]) =>
			isShapeTag(tag)
				? [
						h[tag](
							Object.entries(attributes).map(([key, value]) =>
								h.Attribute(key, String(value)),
							),
						),
					]
				: [],
		),
	);
