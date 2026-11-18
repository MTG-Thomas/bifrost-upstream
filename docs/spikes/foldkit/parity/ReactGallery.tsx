// React half of the design-system parity gallery, built from the shipping
// shadcn components. Content mirrors gallery.ts exactly.
import { useState } from "react";
import { Archive, Copy, Pencil, Trash2 } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

export function ReactGallery() {
  const [workflowName, setWorkflowName] = useState("Nightly Ticket Sync");
  const [confirmation, setConfirmation] = useState("");
  const [lastAction, setLastAction] = useState("None");
  const [isDeleteOpen, setIsDeleteOpen] = useState(false);

  return (
    <main className="grid max-w-xl gap-6 p-6" aria-label="React gallery">
      <div className="flex flex-wrap items-center gap-2">
        <Button>Save Workflow</Button>
        <Button variant="outline">Cancel</Button>
        <Button variant="secondary" disabled>
          Publish
        </Button>
        <Badge>Active</Badge>
        <Badge variant="secondary">Draft</Badge>
      </div>
      <div className="grid min-w-0 gap-2">
        <Label htmlFor="react-workflow-name">Workflow Name</Label>
        <Input
          id="react-workflow-name"
          value={workflowName}
          onChange={(event) => setWorkflowName(event.target.value)}
        />
      </div>
      <div className="flex flex-wrap items-center gap-2">
        <Dialog
          open={isDeleteOpen}
          onOpenChange={(isOpen) => {
            setIsDeleteOpen(isOpen);
            if (isOpen) setConfirmation("");
          }}
        >
          <DialogTrigger asChild>
            <Button variant="destructive">Delete Workflow</Button>
          </DialogTrigger>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Delete Workflow?</DialogTitle>
              <DialogDescription>
                This permanently deletes the workflow and its run history.
              </DialogDescription>
            </DialogHeader>
            <div className="grid min-w-0 gap-2">
              <Label htmlFor="react-delete-confirmation">
                Type the Workflow Name
              </Label>
              <Input
                id="react-delete-confirmation"
                value={confirmation}
                onChange={(event) => setConfirmation(event.target.value)}
              />
            </div>
            <DialogFooter>
              <DialogClose asChild>
                <Button variant="outline">Cancel</Button>
              </DialogClose>
              <Button
                variant="destructive"
                onClick={() => {
                  setLastAction("Deleted Workflow");
                  setIsDeleteOpen(false);
                }}
              >
                Delete
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button variant="outline">Workflow Actions</Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start">
            <DropdownMenuItem onSelect={() => setLastAction("Selected: Edit")}>
              <Pencil />
              Edit
            </DropdownMenuItem>
            <DropdownMenuItem
              onSelect={() => setLastAction("Selected: Duplicate")}
            >
              <Copy />
              Duplicate
            </DropdownMenuItem>
            <DropdownMenuItem disabled>
              <Archive />
              Archive
            </DropdownMenuItem>
            <DropdownMenuSeparator />
            <DropdownMenuLabel>Danger Zone</DropdownMenuLabel>
            <DropdownMenuItem
              variant="destructive"
              onSelect={() => setLastAction("Selected: Delete")}
            >
              <Trash2 />
              Delete
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>
      <p role="status" data-testid="last-action">
        {lastAction}
      </p>
    </main>
  );
}
