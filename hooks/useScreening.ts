import { useState, useCallback, useRef, useEffect } from 'react';
import { useRouter, usePathname } from 'next/navigation';
import { useUser } from './useUser';
import type { PubMedArticle, ScreeningProgress } from '@/types';

// Global state
const globalState = {
  screenedResults: {} as Record<string, PubMedArticle['screeningResult']>,
  tasks: new Map<string, {
    isScreening: boolean,
    isComplete: boolean,
    progress: ScreeningProgress,
    results: Record<string, PubMedArticle['screeningResult']>
  }>(),
  subscribers: new Set<() => void>(),
};

const notifySubscribers = () => {
  globalState.subscribers.forEach(callback => callback());
};

const resetGlobalState = () => {
  globalState.screenedResults = {};
  globalState.tasks.clear();
  notifySubscribers();
};

const getTaskState = (taskId: string) => {
  if (!globalState.tasks.has(taskId)) {
    globalState.tasks.set(taskId, {
      isScreening: false,
      isComplete: false,
      progress: {
        total: 0,
        current: 0,
        currentArticle: null
      },
      results: {}
    });
  }
  return globalState.tasks.get(taskId)!;
};

export function useScreening(setArticles: React.Dispatch<React.SetStateAction<PubMedArticle[]>>) {
  const router = useRouter();
  const pathname = usePathname();
  const { user, loading: userLoading } = useUser();
  const [error, setError] = useState<string | null>(null);
  const [currentTaskId, setCurrentTaskId] = useState<string | null>(null);
  const pollIntervalRef = useRef<NodeJS.Timeout | null>(null);
  const isCancelledRef = useRef(false);

  // Check if we're on the task details page
  const isTaskDetailsPage = pathname?.startsWith('/screening-tasks/');

  useEffect(() => {
    const updateState = () => {
      if (!isTaskDetailsPage && currentTaskId) {
        const taskState = getTaskState(currentTaskId);
        setArticles(prevArticles => 
          prevArticles.map(article => ({
            ...article,
            screeningResult: taskState.results[article.id]
          }))
        );
      }
    };

    globalState.subscribers.add(updateState);
    return () => {
      globalState.subscribers.delete(updateState);
    };
  }, [setArticles, isTaskDetailsPage, currentTaskId]);

  const cleanup = useCallback((taskId: string) => {
    if (pollIntervalRef.current) {
      clearInterval(pollIntervalRef.current);
      pollIntervalRef.current = null;
    }
    isCancelledRef.current = false;
    
    const taskState = getTaskState(taskId);
    taskState.isScreening = false;
    taskState.isComplete = true;
    notifySubscribers();
  }, []);

  const checkTaskStatus = useCallback(async (taskId: string) => {
    if (isCancelledRef.current) {
      cleanup(taskId);
      return;
    }

    try {
      const response = await fetch(`/api/tasks/${taskId}`, {
        credentials: 'include'
      });

      if (response.status === 401) {
        cleanup(taskId);
        setError('Session expired. Please log in again.');
        router.push('/login');
        return;
      }

      if (!response.ok) {
        throw new Error('Failed to fetch task status');
      }

      const data = await response.json();
      if (!data.success) {
        throw new Error(data.message);
      }

      const task = data.task;
      const taskState = getTaskState(taskId);
      
      // Update task progress
      taskState.progress = {
        total: task.progress.total,
        current: task.progress.current,
        currentArticle: null
      };

      // Fetch latest results
      const resultsResponse = await fetch(`/api/tasks/${taskId}/results`, {
        credentials: 'include'
      });

      if (!resultsResponse.ok) {
        throw new Error('Failed to fetch results');
      }

      const resultsData = await resultsResponse.json();
      if (resultsData.success) {
        resultsData.results.forEach((result: any) => {
          taskState.results[result.articleId] = {
            included: result.included,
            reason: result.reason,
            relevanceScore: result.relevanceScore
          };
        });
      }

      notifySubscribers();

      // Check task status
      if (task.status === 'error') {
        cleanup(taskId);
        setError(task.error || 'Screening failed');
        return;
      }

      if (task.status === 'done' || task.status === 'paused') {
        taskState.isComplete = true;
        cleanup(taskId);
      }

    } catch (error) {
      console.error('Error checking task status:', error);
      cleanup(taskId);
      setError('Lost connection to screening service');
    }
  }, [cleanup, router]);

  const screenArticles = useCallback(async (
    criteria: string,
    model: string,
    articlesToScreen: PubMedArticle[],
    allIds: string[],
    _fetchArticleDetails: (ids: string[]) => Promise<PubMedArticle[]>,
    searchQuery: string
  ): Promise<void> => {
    if (userLoading) {
      throw new Error('Loading user data...');
    }

    if (!user?.email) {
      setError('Please log in to start screening');
      router.push('/login');
      return;
    }

    // Always reset state for new screening
    if (currentTaskId) {
      cleanup(currentTaskId);
      setCurrentTaskId(null);
      resetGlobalState();
    }

    setError(null);
    isCancelledRef.current = false;

    try {
      const taskResponse = await fetch('/api/tasks', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({
          userId: user.email,
          searchQuery,
          criteria,
          model,
          totalArticles: allIds.length,
          timestamp: Date.now()
        })
      });

      if (taskResponse.status === 401) {
        throw new Error('Session expired. Please log in again.');
      }

      if (!taskResponse.ok) {
        throw new Error('Failed to create task');
      }

      const taskData = await taskResponse.json();
      if (!taskData.success) {
        throw new Error(taskData.message || 'Failed to create task');
      }

      const taskId = taskData.task._id;
      setCurrentTaskId(taskId);
      
      const taskState = getTaskState(taskId);
      taskState.isScreening = true;
      notifySubscribers();

      const screenResponse = await fetch(`/api/tasks/${taskId}/screen`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({
          articles: articlesToScreen.map(article => ({
            id: article.id,
            title: article.title,
            abstract: article.abstract
          }))
        })
      });

      if (!screenResponse.ok) {
        throw new Error('Failed to start screening');
      }

      pollIntervalRef.current = setInterval(() => {
        if (!isCancelledRef.current) {
          checkTaskStatus(taskId);
        }
      }, 2000);

    } catch (error) {
      await resetScreening();
      const errorMessage = error instanceof Error ? error.message : 'Failed to start screening';
      setError(errorMessage);
      
      if (errorMessage.includes('Session expired') || errorMessage.includes('Please log in')) {
        router.push('/login');
      }
      
      throw error;
    }
  }, [user, userLoading, cleanup, checkTaskStatus, router, currentTaskId]);

  const cancelScreening = useCallback(async () => {
    if (!currentTaskId || isCancelledRef.current) return;

    try {
      isCancelledRef.current = true;
      
      if (pollIntervalRef.current) {
        clearInterval(pollIntervalRef.current);
        pollIntervalRef.current = null;
      }

      const response = await fetch(`/api/tasks/${currentTaskId}/cancel`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include'
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.message || 'Failed to cancel task');
      }

      cleanup(currentTaskId);
      setError('Screening cancelled by user');
      
    } catch (error) {
      console.error('Error cancelling screening:', error);
      setError(error instanceof Error ? error.message : 'Failed to cancel screening');
    }
  }, [cleanup, currentTaskId]);

  const resetScreening = useCallback(async () => {
    if (currentTaskId) {
      cleanup(currentTaskId);
    }
    setCurrentTaskId(null);
    resetGlobalState();
  }, [cleanup, currentTaskId]);

  const taskState = currentTaskId ? getTaskState(currentTaskId) : null;

  return {
    screening: taskState?.isScreening || false,
    screeningError: error,
    screenArticles,
    cancelScreening,
    progress: taskState?.progress || { total: 0, current: 0, currentArticle: null },
    screenedResults: taskState?.results || {},
    currentTaskId,
    completedTaskId: taskState?.isComplete ? currentTaskId : null,
    resetScreening,
    isComplete: taskState?.isComplete || false
  };
}
