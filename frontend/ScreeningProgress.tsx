import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { Play, Loader, CheckCircle2, ExternalLink, RefreshCw, Filter, Save, Cpu } from 'lucide-react';
import { Toast } from '../ui/Toast';
import { ConfirmDialog } from '../ui/ConfirmDialog';

const ARTICLE_LIMIT = 10; // Match with backend limit

interface ScreeningProgressProps {
  total: number;
  current: number;
  currentArticle: string | null;
  isActive: boolean;
  onCancel?: () => void;
  isCancelling?: boolean;
  totalArticles: number;
  taskId?: string | null;
  isSavingArticles?: boolean;
  isInitializingGPU?: boolean;
}

export function ScreeningProgress({
  total,
  current,
  currentArticle,
  isActive,
  onCancel,
  isCancelling = false,
  totalArticles,
  taskId,
  isSavingArticles = false,
  isInitializingGPU = false
}: ScreeningProgressProps) {
  const router = useRouter();
  const [showNewSearchDialog, setShowNewSearchDialog] = useState(false);
  const [showNewCriteriaDialog, setShowNewCriteriaDialog] = useState(false);
  const [showToast, setShowToast] = useState(false);
  const [toastMessage, setToastMessage] = useState('');
  const [taskProgress, setTaskProgress] = useState({ current, total });
  const [processingArticle, setProcessingArticle] = useState<string | null>(null);
  const [taskStatus, setTaskStatus] = useState<'running' | 'done' | 'error' | 'paused'>('running');
  const [taskError, setTaskError] = useState<string | null>(null);
  const [lastUpdateTime, setLastUpdateTime] = useState(Date.now());

  // Fetch real-time progress from database
  useEffect(() => {
    if (!taskId) return;

    const fetchProgress = async () => {
      try {
        const response = await fetch(`/api/tasks/${taskId}`);
        if (!response.ok) {
          throw new Error('Failed to fetch task status');
        }

        const data = await response.json();
        if (!data.success) {
          throw new Error(data.message || 'Failed to fetch task status');
        }

        const task = data.task;
        const now = Date.now();

        // Update task status
        setTaskStatus(task.status);
        if (task.error) {
          setTaskError(task.error);
        }

        // Update processing article
        if (task.currentArticle !== processingArticle) {
          setProcessingArticle(task.currentArticle);
        }

        // Only update progress if status is running or values have changed
        if (task.status === 'running' || 
            task.progress.current !== taskProgress.current || 
            task.progress.total !== taskProgress.total ||
            now - lastUpdateTime > 500) {
          
          setTaskProgress({
            current: task.progress.current,
            total: task.progress.total
          });
          setLastUpdateTime(now);
        }

      } catch (error) {
        console.error('Error fetching progress:', error);
        if (taskStatus === 'running') {
          setTaskError('Lost connection to screening service');
          setTaskStatus('error');
        }
      }
    };

    // Only poll if task is running
    if (taskStatus === 'running') {
      const interval = setInterval(fetchProgress, 1000);
      return () => clearInterval(interval);
    }

    // Single fetch for other states to ensure final values
    if (taskStatus !== 'error') {
      fetchProgress();
    }
  }, [taskId, taskStatus, lastUpdateTime, processingArticle]);

  if (!isActive && taskProgress.current === 0 && !isSavingArticles && !isInitializingGPU) {
    return null;
  }

  // Calculate progress percentage based on limit
  const effectiveTotal = Math.min(total, ARTICLE_LIMIT);
  const progressPercentage = effectiveTotal > 0 ? Math.min(100, (taskProgress.current / effectiveTotal) * 100) : 0;
  
  // Determine completion state
  const isComplete = taskStatus === 'done';
  const isPaused = taskStatus === 'paused';
  const isError = taskStatus === 'error';
  const hasRemainingArticles = totalArticles > ARTICLE_LIMIT;

  const handleViewResults = () => {
    if (taskId) {
      window.open(`/screening-tasks/${taskId}`, '_blank', 'noopener,noreferrer');
    }
  };

  // Show initialization states
  if (isSavingArticles || isInitializingGPU) {
    return (
      <div className="mt-4 p-4 bg-blue-50 border border-blue-100 rounded-lg">
        <div className="flex items-center gap-2 text-blue-700 mb-3">
          <Loader className="h-4 w-4 animate-spin" />
          <span className="text-sm font-medium">Initializing Screening...</span>
        </div>

        <div className="space-y-2">
          {isSavingArticles && (
            <div className="flex items-center gap-2 text-sm text-blue-600">
              <div className="w-8 h-8 rounded-lg bg-blue-100 flex items-center justify-center">
                <Save className="h-4 w-4" />
              </div>
              <span>Saving articles to database...</span>
            </div>
          )}

          {isInitializingGPU && (
            <div className="flex items-center gap-2 text-sm text-blue-600">
              <div className="w-8 h-8 rounded-lg bg-blue-100 flex items-center justify-center">
                <Cpu className="h-4 w-4" />
              </div>
              <span>Waiting for GPU initialization...</span>
            </div>
          )}

          <div className="text-xs text-blue-600 bg-blue-100/50 p-2 rounded">
            <p className="font-medium">Note:</p>
            <p>Processing first {ARTICLE_LIMIT} articles for initial screening.</p>
            {hasRemainingArticles && (
              <p>Additional {totalArticles - ARTICLE_LIMIT} articles will be available for full screening.</p>
            )}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className={`mt-4 p-4 rounded-lg border ${
      isComplete || isPaused ? 'bg-green-50 border-green-100' :
      isError ? 'bg-red-50 border-red-100' :
      'bg-purple-50 border-purple-100'
    }`}>
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          {isComplete ? (
            <CheckCircle2 className="h-4 w-4 text-green-600" />
          ) : isPaused ? (
            <CheckCircle2 className="h-4 w-4 text-green-600" />
          ) : isError ? (
            <ExternalLink className="h-4 w-4 text-red-600" />
          ) : (
            <Loader className="h-4 w-4 text-purple-600 animate-spin" />
          )}
          <span className={`text-sm font-medium ${
            isComplete || isPaused ? 'text-green-700' :
            isError ? 'text-red-700' :
            'text-purple-700'
          }`}>
            {isComplete ? 'Screening Complete!' :
             isPaused ? 'Initial Screening Complete' :
             isError ? 'Screening Error' :
             'Screening Articles...'}
          </span>
        </div>
      </div>

      <div className="space-y-3">
        {processingArticle && taskStatus === 'running' && (
          <div className="p-2 bg-white/50 rounded border border-purple-100">
            <div className="text-sm font-medium text-purple-800">Currently Processing:</div>
            <div className="text-sm text-purple-600">PubMed ID: {processingArticle}</div>
          </div>
        )}

        <div className="flex flex-col gap-1.5">
          <div className="flex justify-between text-sm">
            <span className={`font-medium ${
              isComplete || isPaused ? 'text-green-700' :
              isError ? 'text-red-700' :
              'text-purple-700'
            }`}>
              Progress
            </span>
            <span className={
              isComplete || isPaused ? 'text-green-600' :
              isError ? 'text-red-600' :
              'text-purple-600'
            }>
              {taskProgress.current} of {effectiveTotal} articles
            </span>
          </div>

          <div className={`h-2.5 rounded-full overflow-hidden ${
            isComplete || isPaused ? 'bg-green-100' :
            isError ? 'bg-red-100' :
            'bg-purple-100'
          }`}>
            <div
              className={`h-full transition-all duration-300 ease-out ${
                isComplete || isPaused ? 'bg-green-600' :
                isError ? 'bg-red-600' :
                'bg-purple-600'
              }`}
              style={{ width: `${progressPercentage}%` }}
            />
          </div>

          <div className={`text-xs text-center font-medium ${
            isComplete || isPaused ? 'text-green-500' :
            isError ? 'text-red-500' :
            'text-purple-500'
          }`}>
            {Math.round(progressPercentage)}% Complete
          </div>
        </div>

        {isError && taskError && (
          <div className="p-3 bg-red-100 rounded-lg">
            <p className="text-sm text-red-700">{taskError}</p>
          </div>
        )}

        <div className={`text-xs ${
          isComplete || isPaused ? 'text-green-600 bg-green-100/50' :
          isError ? 'text-red-600 bg-red-100/50' :
          'text-purple-600 bg-purple-100/50'
        } p-2 rounded`}>
          <p className="font-medium">
            {isComplete ? 'Screening Complete!' :
             isPaused ? 'Initial Screening Complete' :
             isError ? 'Screening Error' :
             'Note:'}
          </p>
          <p>{taskProgress.current} articles processed successfully.</p>
          {hasRemainingArticles && !isError && (
            <p>Additional {totalArticles - ARTICLE_LIMIT} articles available for full screening.</p>
          )}
        </div>

        {(isComplete || isPaused || isError) && (
          <div className="flex flex-col gap-2 mt-4">
            <button
              onClick={handleViewResults}
              className="flex items-center justify-center gap-2 w-full px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
            >
              <ExternalLink className="h-4 w-4" />
              View Results
            </button>

            <button
              onClick={() => setShowNewCriteriaDialog(true)}
              className="flex items-center justify-center gap-2 w-full px-4 py-2 bg-purple-600 text-white rounded-lg hover:bg-purple-700 transition-colors"
            >
              <Filter className="h-4 w-4" />
              Screen with Different Criteria
            </button>

            <button
              onClick={() => setShowNewSearchDialog(true)}
              className="flex items-center justify-center gap-2 w-full px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors"
            >
              <RefreshCw className="h-4 w-4" />
              Start New Search
            </button>
          </div>
        )}
      </div>

      <ConfirmDialog
        isOpen={showNewSearchDialog}
        onClose={() => setShowNewSearchDialog(false)}
        onConfirm={() => {
          setShowNewSearchDialog(false);
          window.location.reload();
        }}
        title="Start New Search?"
        message="Starting a new screening will reset the current page and clear all articles. Don't worry - your completed screening results are saved and can be accessed anytime from the Screening Tasks page."
        confirmText="Yes, Start New Screening"
        cancelText="Cancel"
        type="warning"
        icon={RefreshCw}
      />

      <ConfirmDialog
        isOpen={showNewCriteriaDialog}
        onClose={() => setShowNewCriteriaDialog(false)}
        onConfirm={() => {
          setShowNewCriteriaDialog(false);
          const criteriaField = document.querySelector('textarea[placeholder*="screening criteria"]') as HTMLTextAreaElement;
          if (criteriaField) {
            criteriaField.value = '';
            criteriaField.focus();
          }
        }}
        title="Screen with Different Criteria?"
        message="This will allow you to screen the same articles with different criteria. Your previous screening results are saved and can be accessed from the Screening Tasks page."
        confirmText="Enter New Criteria"
        cancelText="Cancel"
        type="info"
        icon={Filter}
      />

      <Toast
        show={showToast}
        message={toastMessage}
        type="success"
        onClose={() => setShowToast(false)}
      />
    </div>
  );
}
