import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { createBrowserRouter } from "react-router";
import { RouterProvider } from "react-router/dom";
import { ApiError } from "./api/client";
import { Layout } from "./components/Layout";
import { Overview } from "./routes/Overview";
import { Queues } from "./routes/Queues";
import { QueueDetail } from "./routes/QueueDetail";
import { Tasks } from "./routes/Tasks";
import { TaskDetail } from "./routes/TaskDetail";
import { NotFound } from "./routes/NotFound";
import "./styles.css";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchInterval: 5_000,
      staleTime: 2_000,
      // Don't retry client errors (bad filter, not found); do retry flaky network.
      retry: (count, error) => !(error instanceof ApiError && error.status < 500) && count < 2,
    },
  },
});

const router = createBrowserRouter([
  {
    element: <Layout />,
    children: [
      { path: "/", element: <Overview /> },
      { path: "/queues", element: <Queues /> },
      { path: "/queues/:name", element: <QueueDetail /> },
      { path: "/tasks", element: <Tasks /> },
      { path: "/tasks/:id", element: <TaskDetail /> },
      { path: "*", element: <NotFound /> },
    ],
  },
]);

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  </StrictMode>,
);
