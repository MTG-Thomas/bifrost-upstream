import "./styles.css";
import { createRoot } from "react-dom/client";

import { mountFoldkitGallery } from "./gallery";
import { ReactGallery } from "./ReactGallery";

const renderer = new URLSearchParams(window.location.search).get("renderer");

if (renderer === "foldkit") {
  const container = document.createElement("div");
  container.id = "foldkit-gallery";
  document.getElementById("foldkit-host")!.appendChild(container);
  mountFoldkitGallery(container);
} else {
  createRoot(document.getElementById("react-root")!).render(<ReactGallery />);
}
