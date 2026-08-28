/**
 * Who Studio is logged in as, and what that lets them do.
 *
 * The session token is persisted so a reload does not log the user out, the way
 * a cookie would not. It is deliberately kept apart from `settings.ts`: the
 * shared runner token is a setting the user types once, while this is an
 * identity the runner hands out and can revoke at any moment.
 *
 * `setRunnerSession` is called wherever the token changes — on rehydrate, on
 * login, on logout — because `lib/runner/client.ts` attaches it to every request
 * from a module-level variable rather than from this store, so that the
 * non-React callers (the SSE readers, the storage backend) do not have to know
 * zustand exists.
 */

import { create } from 'zustand'
import { persist } from 'zustand/middleware'

import {
  createUser as apiCreateUser,
  deleteUser as apiDeleteUser,
  getAuthStatus,
  getMe,
  listRoles,
  listUsers,
  login as apiLogin,
  logout as apiLogout,
  setPassword as apiSetPassword,
  updateUser as apiUpdateUser,
} from '@/lib/auth/client'
import { can as evaluate } from '@/lib/auth/permissions'
import { RunnerError, setRunnerSession } from '@/lib/runner/client'
import type { AuthRole, AuthUser, Principal } from '@/types/auth'
import { useSettingsStore } from '@/store/settings'

interface AuthState {
  /** Whether the runner has users at all. False means the token is the identity. */
  loginRequired: boolean
  /** The session token, or empty. Persisted. */
  session: string
  /** ISO timestamp the session stops working at, as the runner reported it. */
  expiresAt: string
  /** Who the runner says we are, or null when it has not answered yet. */
  principal: Principal | null
  /** The last failure from a call made by this store, for the login form to show. */
  error: string
  /** A call is in flight — used to keep the login gate from flashing on reload. */
  busy: boolean
  /** The status call has come back at least once. */
  ready: boolean

  refresh: () => Promise<void>
  signIn: (username: string, password: string) => Promise<boolean>
  signOut: () => Promise<void>
  clearError: () => void
  can: (action: string, resource?: string) => boolean

  fetchUsers: () => Promise<AuthUser[]>
  fetchRoles: () => Promise<AuthRole[]>
  addUser: (body: {
    username: string
    password: string
    roles: string[]
    displayName?: string
  }) => Promise<void>
  editUser: (userId: string, changes: { roles?: string[]; disabled?: boolean }) => Promise<void>
  changePassword: (userId: string, password: string, currentPassword?: string) => Promise<void>
  removeUser: (userId: string) => Promise<void>
}

/** The runner address and shared token, read at call time so both stay current. */
function runner(): { url: string; token: string } {
  const { runnerUrl, runnerToken } = useSettingsStore.getState()
  return { url: runnerUrl, token: runnerToken }
}

function describe(error: unknown): string {
  if (error instanceof RunnerError) return error.message
  if (error instanceof Error) return error.message
  return 'The local runner could not be reached.'
}

export const useAuthStore = create<AuthState>()(
  persist(
    (set, get) => ({
      loginRequired: false,
      session: '',
      expiresAt: '',
      principal: null,
      error: '',
      busy: false,
      ready: false,

      /**
       * Asks the runner where we stand: does it want a login, and is the session
       * we are holding still good? A session the runner has forgotten is dropped
       * here rather than left to fail on the next run.
       */
      refresh: async () => {
        const { url, token } = runner()
        set({ busy: true })
        try {
          const status = await getAuthStatus(url, token)
          if (!status.loginRequired) {
            // No users: the token is the whole identity, and nothing is hidden.
            setRunnerSession('')
            set({
              loginRequired: false,
              session: '',
              expiresAt: '',
              principal: null,
              error: '',
              ready: true,
            })
            return
          }
          if (status.principal) {
            set({ loginRequired: true, principal: status.principal, error: '', ready: true })
            return
          }
          // It wants a login and does not recognise the session we sent.
          setRunnerSession('')
          set({
            loginRequired: true,
            session: '',
            expiresAt: '',
            principal: null,
            ready: true,
          })
        } catch (error) {
          // An unreachable runner is not a logout: the session may still be good.
          set({ error: describe(error), ready: true })
        } finally {
          set({ busy: false })
        }
      },

      signIn: async (username, password) => {
        const { url, token } = runner()
        set({ busy: true, error: '' })
        try {
          const session = await apiLogin(url, { username, password }, token)
          setRunnerSession(session.token)
          set({
            loginRequired: true,
            session: session.token,
            expiresAt: session.expiresAt,
            principal: session.principal,
            error: '',
            ready: true,
          })
          return true
        } catch (error) {
          set({ error: describe(error) })
          return false
        } finally {
          set({ busy: false })
        }
      },

      /**
       * Drops the session locally whatever the runner says. If the call failed
       * because the runner is down, the user still wanted to be logged out here.
       */
      signOut: async () => {
        const { url, token } = runner()
        const held = get().session
        set({ session: '', expiresAt: '', principal: null, error: '' })
        setRunnerSession('')
        if (!held) return
        try {
          setRunnerSession(held)
          await apiLogout(url, token)
        } catch {
          /* the session is gone from this browser either way */
        } finally {
          setRunnerSession('')
        }
      },

      clearError: () => set({ error: '' }),

      can: (action, resource = '*') => evaluate(get().principal, action, resource),

      fetchUsers: async () => {
        const { url, token } = runner()
        return listUsers(url, token)
      },

      fetchRoles: async () => {
        const { url, token } = runner()
        return listRoles(url, token)
      },

      addUser: async (body) => {
        const { url, token } = runner()
        await apiCreateUser(url, body, token)
        // The first user turns a token-only runner into one that wants a login.
        await get().refresh()
      },

      editUser: async (userId, changes) => {
        const { url, token } = runner()
        await apiUpdateUser(url, userId, changes, token)
      },

      changePassword: async (userId, password, currentPassword) => {
        const { url, token } = runner()
        await apiSetPassword(url, userId, { password, currentPassword }, token)
        // Changing a password ends every session it opened, including this one.
        if (get().principal?.userId === userId) {
          setRunnerSession('')
          set({ session: '', expiresAt: '', principal: null })
        }
      },

      removeUser: async (userId) => {
        const { url, token } = runner()
        await apiDeleteUser(url, userId, token)
      },
    }),
    {
      name: 'sparquet-studio:auth',
      version: 1,
      // The principal is not persisted: it is the runner's answer, and a stale
      // copy would decide what the UI offers. Only the token survives a reload.
      partialize: (state) => ({
        session: state.session,
        expiresAt: state.expiresAt,
        loginRequired: state.loginRequired,
      }),
      onRehydrateStorage: () => (state) => {
        if (state) setRunnerSession(state.session)
      },
    },
  ),
)

/** Reads the current principal outside React, where a hook cannot be called. */
export function currentPrincipal(): Principal | null {
  return useAuthStore.getState().principal
}

/** `can` for non-React callers. Same permissive default as `lib/auth/permissions`. */
export function can(action: string, resource = '*'): boolean {
  return useAuthStore.getState().can(action, resource)
}

/** Refetches `getMe` after a role change, so the UI stops offering what was revoked. */
export async function refreshPrincipal(): Promise<void> {
  const { runnerUrl, runnerToken } = useSettingsStore.getState()
  try {
    const principal = await getMe(runnerUrl, runnerToken)
    useAuthStore.setState({ principal })
  } catch {
    /* leave the last known principal in place; the runner still decides */
  }
}
