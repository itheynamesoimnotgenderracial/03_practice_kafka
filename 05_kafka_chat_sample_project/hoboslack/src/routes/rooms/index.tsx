import { createFileRoute } from '@tanstack/react-router'

export const Route = createFileRoute('/rooms/')({
  component: RouteComponent,
})

function RouteComponent() {
  return <div style={{
    height: "100vh"
  }}>Hello "/rooms"!</div>
}
