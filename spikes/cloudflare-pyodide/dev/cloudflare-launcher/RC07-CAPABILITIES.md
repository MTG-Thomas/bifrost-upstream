# Container Guest Capability Initialization

Hosted attempt 7 on rc06 reported a signed `guest-init` / `capabilities`
failure. The child had `CapInh == CapBnd == 0x00000000abac35fb` and zero
effective, permitted, and ambient masks. The supervisor’s inheritable mask
was zero after its own drop. The UID transition before guest `execve` clears
effective, permitted, and ambient capabilities but preserves inheritable
capabilities. Docker ordinarily starts image init with an empty inheritable
set, so the normal offline fixture missed the difference.

Image init now checks that it is single-threaded, clears both words of the
inheritable set with `capset`, and explicitly clears ambient capabilities.
It drops every bit present in its bounding set before it spawns the guest.
This uses the init process’s effective `CAP_SETPCAP`. The drop only limits
future `execve` and additions to the inheritable set. Its current effective
and permitted masks still cover `setgroups`, `setgid`, and `setuid` in the
forked child and the supervisor’s own UID/GID transition. The supervisor
then keeps `CAP_KILL` in its current permitted and effective sets for guest
cleanup, with zero inheritable, ambient, and bounding masks. A syscall
failure aborts initialization. [Linux capabilities(7)](https://man7.org/linux/man-pages/man7/capabilities.7.html)

The child still enforces the existing UID, GID, groups, `no_new_privs`, and
zero inheritable, permitted, effective, and ambient masks. Qualification
evidence already includes `CapBnd`. Keep it so the Docker rehearsal and
the next hosted qualification can verify the reduced bounding mask without
a new wire field. Capacity and SDK protocol versions are unchanged.

The local `--containers-hosted-shape --exercise-hosted-capabilities`
rehearsal sets PID 1’s inheritable mask equal to its nonempty Docker bounding
mask before the launcher spawns its guest. It checks zero masks on the guest,
the supervisor’s retained `CAP_KILL`, both settled canaries, the overflow
workflow, and teardown. The separate `--exercise-guest-init-fault capabilities`
fixture deliberately retains one bounding bit and adds it back to the
inheritable mask at the spawn boundary. That verifies the guest still
refuses a process with nonzero inheritable capabilities.
