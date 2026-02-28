import { createFileRoute } from '@tanstack/react-router'

export const Route = createFileRoute('/rooms/$roomId')({
  component: RouteComponent,
})

function RouteComponent() {
  return <div>Hello "/rooms/$roomId"!</div>
}
