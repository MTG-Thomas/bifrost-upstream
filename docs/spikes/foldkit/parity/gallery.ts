// Foldkit half of the design-system parity gallery. ReactGallery.tsx renders
// the same controls with the shipping shadcn components; run-parity.mjs drives
// both with identical keyboard and pointer scenarios.
import { Option, Schema } from "effect";
import { Runtime, Update } from "foldkit";
import type { Html, HtmlBuilder } from "foldkit/html";
import { defineMessageUnion } from "foldkit/message";
import { modifyFields } from "foldkit/struct";
import { Dialog, Menu } from "@foldkit/ui";
import { Archive, Copy, Pencil, Trash2 } from "lucide";

import { buttonVariants } from "@/components/ui/button-variants";
import * as BifrostBadge from "@/foldkit/ui/badge";
import * as BifrostButton from "@/foldkit/ui/button";
import * as BifrostDialog from "@/foldkit/ui/dialog";
import * as BifrostDropdownMenu from "@/foldkit/ui/dropdownMenu";
import { icon } from "@/foldkit/ui/icons";
import * as BifrostInput from "@/foldkit/ui/input";

// MODEL

const Action = Schema.Literals(["Edit", "Duplicate", "Archive", "Delete"]);
type Action = typeof Action.Type;
const ACTIONS: ReadonlyArray<Action> = [
  "Edit",
  "Duplicate",
  "Archive",
  "Delete",
];

const Model = Schema.Struct({
  workflowName: Schema.String,
  confirmation: Schema.String,
  lastAction: Schema.String,
  deleteDialog: Dialog.Model,
  actionsMenu: Menu.Model,
});
type Model = typeof Model.Type;

// MESSAGE

const Message = defineMessageUnion({
  UpdatedWorkflowName: { value: Schema.String },
  UpdatedConfirmation: { value: Schema.String },
  ClickedDeleteWorkflow: {},
  ClickedConfirmDelete: {},
  GotDeleteDialogMessage: { message: Dialog.Message },
  GotActionsMenuMessage: { message: Menu.Message },
});
type Message = typeof Message.Type;

// UPDATE

type UpdateReturn = Update.Return<Model, Message>;

const ActionsMenu = Menu.create<Action>();

const foldDeleteDialog = Update.foldChild({
  update: Dialog.update,
  read: (model: Model) => Option.some(model.deleteDialog),
  write: (model, nextDeleteDialog) =>
    modifyFields(model, { deleteDialog: () => nextDeleteDialog }),
  toParentMessage: (message) => Message.GotDeleteDialogMessage({ message }),
  foldOutMessage: Dialog.OutMessage.match<Update.Step<Model, Message>>({
    Opened: () => (model) => ({ model }),
    Closed: () => (model) => ({ model }),
  }),
});

const stepDeleteDialog = (
  update: (
    model: Dialog.Model,
  ) => Update.ReturnWithOutMessage<
    Dialog.Model,
    Dialog.Message,
    Dialog.OutMessage
  >,
) =>
  Update.foldChildStep({
    update,
    read: (model: Model) => Option.some(model.deleteDialog),
    write: (model, nextDeleteDialog) =>
      modifyFields(model, { deleteDialog: () => nextDeleteDialog }),
    toParentMessage: (message) => Message.GotDeleteDialogMessage({ message }),
    foldOutMessage: Dialog.OutMessage.match<Update.Step<Model, Message>>({
      Opened: () => (model) => ({ model }),
      Closed: () => (model) => ({ model }),
    }),
  });

const openDeleteDialog = stepDeleteDialog(Dialog.open);
const closeDeleteDialog = stepDeleteDialog(Dialog.close);

const foldActionsMenu = Update.foldChild({
  update: ActionsMenu.update,
  read: (model: Model) => Option.some(model.actionsMenu),
  write: (model, nextActionsMenu) =>
    modifyFields(model, { actionsMenu: () => nextActionsMenu }),
  toParentMessage: (message) => Message.GotActionsMenuMessage({ message }),
  foldOutMessage: Menu.OutMessage.match<
    Update.Step<Model, Message>,
    Menu.OutMessage<Action>
  >({
    Selected:
      ({ value }) =>
      (model) => ({
        model: modifyFields(model, { lastAction: () => `Selected: ${value}` }),
      }),
  }),
});

const update = (model: Model, message: Message): UpdateReturn =>
  Message.match<UpdateReturn>(message, {
    UpdatedWorkflowName: ({ value }) => ({
      model: modifyFields(model, { workflowName: () => value }),
    }),
    UpdatedConfirmation: ({ value }) => ({
      model: modifyFields(model, { confirmation: () => value }),
    }),
    ClickedDeleteWorkflow: () =>
      openDeleteDialog(modifyFields(model, { confirmation: () => "" })),
    ClickedConfirmDelete: () =>
      closeDeleteDialog(
        modifyFields(model, { lastAction: () => "Deleted Workflow" }),
      ),
    GotDeleteDialogMessage: ({ message }) => foldDeleteDialog(model, message),
    GotActionsMenuMessage: ({ message }) => foldActionsMenu(model, message),
  });

// VIEW

const actionIcon = { Edit: Pencil, Duplicate: Copy, Archive, Delete: Trash2 };

const view = (model: Model, h: HtmlBuilder<Message>): Html =>
  h.main(
    [h.Class("grid max-w-xl gap-6 p-6"), h.AriaLabel("Foldkit gallery")],
    [
      h.div(
        [h.Class("flex flex-wrap items-center gap-2")],
        [
          BifrostButton.view({ content: ["Save Workflow"] }, h),
          BifrostButton.view({ variant: "outline", content: ["Cancel"] }, h),
          BifrostButton.view(
            { variant: "secondary", isDisabled: true, content: ["Publish"] },
            h,
          ),
          BifrostBadge.view({ content: ["Active"] }, h),
          BifrostBadge.view({ variant: "secondary", content: ["Draft"] }, h),
        ],
      ),
      BifrostInput.view(
        {
          id: "foldkit-workflow-name",
          label: "Workflow Name",
          value: model.workflowName,
          onInput: (value) => Message.UpdatedWorkflowName({ value }),
        },
        h,
      ),
      h.div(
        [h.Class("flex flex-wrap items-center gap-2")],
        [
          BifrostButton.view(
            {
              variant: "destructive",
              onClick: Message.ClickedDeleteWorkflow(),
              content: ["Delete Workflow"],
            },
            h,
          ),
          BifrostDropdownMenu.view(
            {
              menu: ActionsMenu,
              model: model.actionsMenu,
              toParentMessage: (message) =>
                Message.GotActionsMenuMessage({ message }),
              items: ACTIONS,
              trigger: ["Workflow Actions"],
              itemContent: (item) => [icon(actionIcon[item], "", h), item],
              isItemDisabled: (item) => item === "Archive",
              isItemDestructive: (item) => item === "Delete",
              itemGroupKey: (item) =>
                item === "Delete" ? "Danger" : "Actions",
              groupLabel: (group) =>
                group === "Danger" ? "Danger Zone" : undefined,
            },
            h,
          ),
        ],
      ),
      h.p(
        [h.Role("status"), h.DataAttribute("testid", "last-action")],
        [model.lastAction],
      ),
      BifrostDialog.view(
        {
          model: model.deleteDialog,
          toParentMessage: (message) =>
            Message.GotDeleteDialogMessage({ message }),
          title: "Delete Workflow?",
          description:
            "This permanently deletes the workflow and its run history.",
          body: () => [
            BifrostInput.view(
              {
                id: "foldkit-delete-confirmation",
                label: "Type the Workflow Name",
                value: model.confirmation,
                onInput: (value) => Message.UpdatedConfirmation({ value }),
              },
              h,
            ),
          ],
          footer: (render) => [
            h.button(
              [
                ...render.closeButton,
                h.Class(buttonVariants({ variant: "outline" })),
              ],
              ["Cancel"],
            ),
            BifrostButton.view(
              {
                variant: "destructive",
                onClick: Message.ClickedConfirmDelete(),
                content: ["Delete"],
              },
              h,
            ),
          ],
        },
        h,
      ),
    ],
  );

// PROGRAM

export const mountFoldkitGallery = (container: HTMLElement) =>
  Runtime.embed(
    Runtime.makeElement({
      Model,
      init: () => ({
        model: {
          workflowName: "Nightly Ticket Sync",
          confirmation: "",
          lastAction: "None",
          deleteDialog: Dialog.init({
            id: "foldkit-delete-dialog",
            isAnimated: true,
          }),
          actionsMenu: Menu.init({
            id: "foldkit-actions-menu",
            isAnimated: true,
          }),
        },
      }),
      update,
      view,
      container,
      devTools: false,
    }),
  );
