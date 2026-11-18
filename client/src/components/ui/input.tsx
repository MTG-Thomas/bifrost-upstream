import * as React from "react"

import { cn } from "@/lib/utils"
import { inputClassName } from "./input-variants"

function Input({ className, type, ...props }: React.ComponentProps<"input">) {
  return (
    <input
      type={type}
      data-slot="input"
      className={cn(
        inputClassName,
        className
      )}
      {...props}
    />
  )
}

export { Input }
