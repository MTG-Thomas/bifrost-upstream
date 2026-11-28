# Container Guest Capability Initialization

Hosted attempt 7 on rc06 reported a signed `guest-init` / `capabilities`
failure. The child had `CapInh == CapBnd == 0x00000000abac35fb` and zero
effective, permitted, and ambient masks. The supervisor’s inheritable mask
was zero after its own drop. The UID transition before guest `execve` clears
effective, permitted, and ambient capabilities but preserves inheritable
capabilities. Docker ordinarily starts image init with an empty inheritable
set, so the normal offline fixture missed the difference.

Image init checks that it is single-threaded and clears both words of the
inheritable set with `capset`. The kernel clears ambient capabilities when
their corresponding inheritable or permitted capability is lowered.
It drops every bit present in its bounding set before it spawns the guest.
This uses the init process’s effective `CAP_SETPCAP`. The drop only limits
future `execve` and additions to the inheritable set. Its current effective
and permitted masks still cover `setgroups`, `setgid`, and `setuid` in the
forked child and the supervisor’s own UID/GID transition. The supervisor
then keeps `CAP_KILL` in its current permitted and effective sets for guest
cleanup, with zero inheritable, ambient, and bounding masks. A syscall
failure aborts initialization. [Linux capabilities(7)](https://man7.org/linux/man-pages/man7/capabilities.7.html)

If a supervisor syscall fails, the launcher records a fixed step and errno
class, plus the bit number for a bounding-set drop. The steps before spawn
abort before any guest process exists. The child receives permission to
initialize only after the supervisor finishes its credential drop. A failure
between spawn and that grant closes the work pipe, so the waiting child exits.

The launcher can connect to report the failure only after verifying its
non-root UID, GID, empty supplementary groups, and zero effective,
permitted, inheritable, and ambient masks. It protects the per-incarnation
credential before use, performs signed hello with an empty handler list,
sends one signed `guest_fatal` at `guest-init`, and exits with status 1.
If the identity cannot shed its privileges or the credential cannot be
protected, only the local log is available. A failed hello also ends the
diagnostic attempt. The API stores successful reports in the existing
qualification evidence.

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

The `supervisor-seccomp` and `supervisor-seccomp-partial` fault modes
install a real seccomp filter in PID 1 before launch. They deny
`prctl(PR_CAPBSET_DROP)` on the first or second bit, respectively, and
verify the signed failure receipt and cleanup through Disconnect.
