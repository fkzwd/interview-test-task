package com.vk.dwzkf.test.impl;

import com.vk.dwzkf.test.Accumulator;
import com.vk.dwzkf.test.State;
import com.vk.dwzkf.test.StateObject;

import java.util.*;

import static com.vk.dwzkf.test.State.*;

/**
 * @author Roman Shageev
 * @since 12.08.2024
 */
public class AccumulatorImpl implements Accumulator {

    private final Map<Long, List<StateObject>> processMap = new HashMap<>();
    private final Map<Long, State> finalStatesMap = new HashMap<>();
    private int counter = 0;
    private int totalAccepted = 0;

    @Override
    public void accept(StateObject stateObject) {
        Long processId = stateObject.getProcessId();

        if (shouldRemoveProcess(stateObject, processId)) {
            processMap.remove(processId);
        }

        addStateObject(processId, stateObject);
        updateFinalState(processId, stateObject);
        totalAccepted++;
    }

    @Override
    public void acceptAll(List<StateObject> stateObjects) {
        stateObjects.forEach(this::accept);
    }

    @Override
    public List<StateObject> drain(Long processId) {
        List<StateObject> stateObjects = getStateObjectsForProcess(processId);
        if (stateObjects.isEmpty()) {
            return List.of();
        }

        if (shouldReturnFinalStatesOnly(stateObjects)) {
            return filterFinalStates(stateObjects);
        }

        List<StateObject> uniqueStateObjects = removeDuplicateSeqNo(stateObjects);
        sortStateObjects(uniqueStateObjects);

        counter++;
        return buildValidStateSequence(uniqueStateObjects);
    }

    /**
     * Проверяет, нужно ли удалить процесс на основе достигнутого состояния.
     */
    private boolean shouldRemoveProcess(StateObject stateObject, Long processId) {
        return isFinalStateReached(processId, stateObject) && counter > 0;
    }

    /**
     * Добавляет объект состояния в карту процессов.
     */
    private void addStateObject(Long processId, StateObject stateObject) {
        processMap.computeIfAbsent(processId, k -> new ArrayList<>()).add(stateObject);
    }

    /**
     * Обновляет финальное состояние процесса.
     */
    private void updateFinalState(Long processId, StateObject stateObject) {
        if (stateObject.getState() == FINAL1 || stateObject.getState() == FINAL2) {
            finalStatesMap.put(processId, stateObject.getState());
        }
    }

    /**
     * Проверяет, достигло ли состояние процесса финального состояния.
     */
    private boolean isFinalStateReached(Long processId, StateObject stateObject) {
        State existingFinalState = finalStatesMap.get(processId);
        if (existingFinalState != null) {
            if (existingFinalState == FINAL1) {
                return true;
            } else if (existingFinalState == FINAL2 && stateObject.getState() == FINAL1) {
                finalStatesMap.put(processId, FINAL1);
                replaceFinal2WithFinal1(processId, stateObject);
                return true;
            } else return existingFinalState == FINAL2;
        }
        return false;
    }

    /**
     * Проверяет, следует ли вернуть только финальные состояния.
     */
    private boolean shouldReturnFinalStatesOnly(List<StateObject> stateObjects) {
        return !hasValidTransitions(stateObjects) && stateObjects.size() > 1;
    }

    /**
     * Фильтрует и возвращает только финальные состояния.
     */
    private static List<StateObject> filterFinalStates(List<StateObject> stateObjects) {
        List<State> states = stateObjects.stream()
                .map(StateObject::getState)
                .filter(state -> state == FINAL1 || state == FINAL2)
                .toList();
        if (states.contains(FINAL1) || states.contains(FINAL2)) {
            return stateObjects.stream()
                    .filter(so -> so.getState() == FINAL1 || so.getState() == FINAL2)
                    .toList();
        }
        return List.of();
    }

    /**
     * Извлекает список состояний для заданного идентификатора процесса.
     */
    private List<StateObject> getStateObjectsForProcess(Long processId) {
        return processMap.getOrDefault(processId, new ArrayList<>());
    }

    /**
     * Удаляет дубликаты состояний на основе номера последовательности (seqNo).
     */
    private List<StateObject> removeDuplicateSeqNo(List<StateObject> stateObjects) {
        Map<Integer, StateObject> seqNoMap = new LinkedHashMap<>();
        for (StateObject so : stateObjects) {
            seqNoMap.putIfAbsent(so.getSeqNo(), so);
        }
        return new ArrayList<>(seqNoMap.values());
    }

    /**
     * Сортирует состояния по приоритету и номеру последовательности.
     */
    private void sortStateObjects(List<StateObject> stateObjects) {
        stateObjects.sort((o1, o2) -> {
            int priorityCompare = compareStatePriority(o1.getState(), o2.getState());
            if (priorityCompare == 0) {
                return Integer.compare(o1.getSeqNo(), o2.getSeqNo());
            }
            return priorityCompare;
        });

        reorderMidStates(stateObjects);
    }

    /**
     * Переставляет состояния MID1 и MID2, если они идут в неверном порядке.
     */
    private void reorderMidStates(List<StateObject> stateObjects) {
        for (int i = 1; i < stateObjects.size() - 1; i++) {
            StateObject prev = stateObjects.get(i - 1);
            StateObject current = stateObjects.get(i);
            StateObject next = stateObjects.get(i + 1);

            if ((current.getState() == MID2 && next.getState() == MID1) &&
                    (prev.getState() == START1 || prev.getState() == START2)) {
                stateObjects.set(i, next);
                stateObjects.set(i + 1, current);
            }
        }
    }

    /**
     * Строит и возвращает последовательность состояний, начиная с последнего состояния.
     */
    private List<StateObject> buildValidStateSequence(List<StateObject> stateObjects) {
        List<StateObject> result = new ArrayList<>();
        State lastState = null;

        for (StateObject so : stateObjects) {
            if (lastState == null) {
                if (isStartState(so.getState())) {
                    result.add(so);
                    lastState = so.getState();
                }
            } else {
                if (isValidTransition(lastState, so.getState())) {
                    result.add(so);
                    lastState = so.getState();
                    if (isFinalState(so.getState())) {
                        break;
                    }
                }
            }
        }

        List<StateObject> recentStateObjects = result.subList(
                Math.max(result.size() - totalAccepted, 0),
                result.size()
        );

        totalAccepted = 0;
        return recentStateObjects;
    }

    /**
     * Проверяет, является ли состояние начальным (START1 или START2).
     */
    private boolean isStartState(State state) {
        return state == START1 || state == START2;
    }

    /**
     * Проверяет, является ли состояние финальным (FINAL1 или FINAL2).
     */
    private boolean isFinalState(State state) {
        return state == FINAL1 || state == FINAL2;
    }

    /**
     * Заменяет состояние FINAL2 на FINAL1 в списке состояний процесса.
     */
    private void replaceFinal2WithFinal1(Long processId, StateObject final1StateObject) {
        List<StateObject> stateObjects = processMap.get(processId);
        if (stateObjects != null) {
            for (int i = 0; i < stateObjects.size(); i++) {
                if (stateObjects.get(i).getState() == FINAL2) {
                    stateObjects.set(i, final1StateObject);
                    break;
                }
            }
        }
    }

    /**
     * Сравнивает приоритеты состояний.
     */
    private int compareStatePriority(State state1, State state2) {
        if ((state1 == MID1 && state2 == MID2) || (state1 == MID2 && state2 == MID1)) {
            return 0;
        }
        List<State> priorityList = Arrays.asList(START1, START2, MID1, MID2, FINAL1, FINAL2);
        return Integer.compare(priorityList.indexOf(state1), priorityList.indexOf(state2));
    }

    /**
     * Проверяет, есть ли допустимые переходы между состояниями.
     */
    private boolean hasValidTransitions(List<StateObject> stateObjects) {
        Set<State> presentStates = new HashSet<>();
        for (StateObject stateObject : stateObjects) {
            presentStates.add(stateObject.getState());
        }

        for (StateObject stateObject : stateObjects) {
            State state = stateObject.getState();
            List<State> validNextStates = getValidNextStates(state);

            // Проверяем, есть ли хотя бы одно состояние, которое может следовать за текущим
            for (State validNextState : validNextStates) {
                if (presentStates.contains(validNextState)) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Возвращает список допустимых следующих состояний.
     */
    private List<State> getValidNextStates(State state) {
        Map<State, List<State>> validTransitions = new HashMap<>();
        validTransitions.put(START1, Arrays.asList(MID1, FINAL1));
        validTransitions.put(START2, Arrays.asList(MID1, FINAL2));
        validTransitions.put(MID1, Arrays.asList(MID2, FINAL1, FINAL2));
        validTransitions.put(MID2, Arrays.asList(MID1, FINAL1, FINAL2));
        validTransitions.put(FINAL1, Collections.emptyList());
        validTransitions.put(FINAL2, Collections.emptyList());

        return validTransitions.getOrDefault(state, Collections.emptyList());
    }

    /**
     * Проверяет, является ли переход между состояниями допустимым.
     */
    private boolean isValidTransition(State from, State to) {
        Map<State, List<State>> validTransitions = new HashMap<>();
        validTransitions.put(START1, Arrays.asList(MID1, FINAL1, FINAL2));
        validTransitions.put(START2, Arrays.asList(MID1, FINAL1, FINAL2));
        validTransitions.put(MID1, Arrays.asList(MID2, FINAL1, FINAL2));
        validTransitions.put(MID2, Arrays.asList(MID1, FINAL1, FINAL2));
        validTransitions.put(FINAL1, Collections.emptyList());
        validTransitions.put(FINAL2, Collections.emptyList());

        return validTransitions.getOrDefault(from, Collections.emptyList()).contains(to);
    }
}
